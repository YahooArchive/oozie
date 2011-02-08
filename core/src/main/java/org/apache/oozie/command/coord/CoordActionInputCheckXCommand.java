/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License. See accompanying LICENSE file.
 */
package org.apache.oozie.command.coord;

import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.service.HadoopAccessorException;
import org.apache.oozie.service.HadoopAccessorService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

public class CoordActionInputCheckXCommand extends CoordinatorXCommand<Void> {

    private String actionId;
    private static XLog log = XLog.getLog(CoordActionInputCheckXCommand.class);
    private int COMMAND_REQUEUE_INTERVAL = 60000; // 1 minute
    private CoordinatorActionBean coordAction = null;
    private JPAService jpaService = null;

    public CoordActionInputCheckXCommand(String actionId) {
        super("coord_action_input", "coord_action_input", 1);
        this.actionId = actionId;
    }

    @Override
    protected Void execute() throws CommandException {
        log.info("[" + actionId + "]::ActionInputCheck:: Action is in WAITING state.");
        StringBuilder actionXml = new StringBuilder(coordAction.getActionXml());// job.getXml();
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            cron.start();
            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            CoordCommandUtils.getResolvedList(coordAction.getMissingDependencies(), nonExistList,
                                                          nonResolvedList);

            log.info("[" + actionId + "]::ActionInputCheck:: Missing deps:" + nonExistList.toString() + " "
                    + nonResolvedList.toString());
            Date actualTime = new Date();
            boolean status = checkInput(actionXml, existList, nonExistList, actionConf, actualTime);
            coordAction.setLastModifiedTime(actualTime);
            coordAction.setActionXml(actionXml.toString());
            if (nonResolvedList.length() > 0 && status == false) {
                nonExistList.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR).append(
                        nonResolvedList);
            }
            coordAction.setMissingDependencies(nonExistList.toString());
            if (status == true) {
                coordAction.setStatus(CoordinatorAction.Status.READY);
                // pass jobID to the ReadyCommand
                queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
            }
            else {
                long waitingTime = (actualTime.getTime() - coordAction.getNominalTime().getTime()) / (60 * 1000);
                int timeOut = coordAction.getTimeOut();
                if ((timeOut >= 0) && (waitingTime > timeOut)) {
                    queue(new CoordActionTimeOutXCommand(coordAction), 100);
                    coordAction.setStatus(CoordinatorAction.Status.TIMEDOUT);
                }
                else {
                    queue(new CoordActionInputCheckXCommand(coordAction.getId()), COMMAND_REQUEUE_INTERVAL);
                }
            }
            coordAction.setLastModifiedTime(new Date());
            jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1005, e.getMessage(), e);
        }
        cron.stop();

        return null;
    }

    protected boolean checkInput(StringBuilder actionXml, StringBuilder existList, StringBuilder nonExistList,
                                 Configuration conf, Date actualTime) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        boolean allExist = checkResolvedUris(eAction, existList, nonExistList, conf);
        if (allExist) {
            log.debug("[" + actionId + "]::ActionInputCheck:: Checking Latest/future");
            allExist = checkUnresolvedInstances(eAction, conf, actualTime);
        }
        if (allExist == true) {
            materializeDataProperties(eAction, conf);
            actionXml.replace(0, actionXml.length(), XmlUtils.prettyPrint(eAction).toString());
        }
        return allExist;
    }

    /**
     * Materialize data properties defined in <action> tag. it includes dataIn(<DS>) and dataOut(<DS>) it creates a list
     * of files that will be needed.
     *
     * @param eAction
     * @param conf
     * @throws Exception
     * @update modify 'Action' element with appropriate list of files.
     */
    private void materializeDataProperties(Element eAction, Configuration conf) throws Exception {
        ELEvaluator eval = CoordELEvaluator.createDataEvaluator(eAction, conf, actionId);
        Element configElem = eAction.getChild("action", eAction.getNamespace()).getChild("workflow",
                                                                                         eAction.getNamespace()).getChild("configuration", eAction.getNamespace());
        if (configElem != null) {
            for (Element propElem : (List<Element>) configElem.getChildren("property", configElem.getNamespace())) {
                resolveTagContents("value", propElem, eval);
            }
        }
    }

    private void resolveTagContents(String tagName, Element elem, ELEvaluator eval) throws Exception {
        if (elem == null) {
            return;
        }
        Element tagElem = elem.getChild(tagName, elem.getNamespace());
        if (tagElem != null) {
            String updated = CoordELFunctions.evalAndWrap(eval, tagElem.getText());
            tagElem.removeContent();
            tagElem.addContent(updated);
        }
        else {
            log.warn(" Value NOT FOUND " + tagName);
        }
    }

    private boolean checkUnresolvedInstances(Element eAction, Configuration actionConf, Date actualTime)
            throws Exception {
        String strAction = XmlUtils.prettyPrint(eAction).toString();
        Date nominalTime = DateUtils.parseDateUTC(eAction.getAttributeValue("action-nominal-time"));
        StringBuffer resultedXml = new StringBuffer();

        boolean ret;
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            ret = materializeUnresolvedEvent(inputList.getChildren("data-in", eAction.getNamespace()),
                                             nominalTime, actualTime, actionConf);
            if (ret == false) {
                resultedXml.append(strAction);
                return false;
            }
        }

        // Using latest() or future() in output-event is not intuitive.
        // We need to make
        // sure, this assumption is correct.
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element dEvent : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (dEvent.getChild("unresolved-instances", dEvent.getNamespace()) != null) {
                    throw new CommandException(ErrorCode.E1006, "coord:latest()/future()",
                            " not permitted in output-event ");
                }
            }
            /*
             * ret = materializeUnresolvedEvent( (List<Element>)
             * outputList.getChildren("data-out", eAction.getNamespace()),
             * actualTime, nominalTime, actionConf); if (ret == false) {
             * resultedXml.append(strAction); return false; }
             */
        }
        return true;
    }

    private boolean materializeUnresolvedEvent(List<Element> eDataEvents, Date nominalTime, Date actualTime,
                                               Configuration conf) throws Exception {
        for (Element dEvent : eDataEvents) {
            if (dEvent.getChild("unresolved-instances", dEvent.getNamespace()) == null) {
                continue;
            }
            ELEvaluator eval = CoordELEvaluator.createLazyEvaluator(actualTime, nominalTime, dEvent, conf);
            String uresolvedInstance = dEvent.getChild("unresolved-instances", dEvent.getNamespace()).getTextTrim();
            String unresolvedList[] = uresolvedInstance.split(CoordELFunctions.INSTANCE_SEPARATOR);
            StringBuffer resolvedTmp = new StringBuffer();
            for (int i = 0; i < unresolvedList.length; i++) {
                String ret = CoordELFunctions.evalAndWrap(eval, unresolvedList[i]);
                Boolean isResolved = (Boolean) eval.getVariable("is_resolved");
                if (isResolved == false) {
                    log.info("[" + actionId + "]::Cannot resolve: " + ret);
                    return false;
                }
                if (resolvedTmp.length() > 0) {
                    resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                resolvedTmp.append((String) eval.getVariable("resolved_path"));
            }
            if (resolvedTmp.length() > 0) {
                if (dEvent.getChild("uris", dEvent.getNamespace()) != null) {
                    resolvedTmp.append(CoordELFunctions.INSTANCE_SEPARATOR).append(
                            dEvent.getChild("uris", dEvent.getNamespace()).getTextTrim());
                    dEvent.removeChild("uris", dEvent.getNamespace());
                }
                Element uriInstance = new Element("uris", dEvent.getNamespace());
                uriInstance.addContent(resolvedTmp.toString());
                dEvent.getContent().add(1, uriInstance);
            }
            dEvent.removeChild("unresolved-instances", dEvent.getNamespace());
        }

        return true;
    }

    private boolean checkResolvedUris(Element eAction, StringBuilder existList, StringBuilder nonExistList,
                                      Configuration conf) throws IOException {

        log.info("[" + actionId + "]::ActionInputCheck:: In checkResolvedUris...");
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            // List<Element> eDataEvents = inputList.getChildren("data-in",
            // eAction.getNamespace());
            // for (Element event : eDataEvents) {
            // Element uris = event.getChild("uris", event.getNamespace());
            if (nonExistList.length() > 0) {
                checkListOfPaths(existList, nonExistList, conf);
            }
            // }
            return nonExistList.length() == 0;
        }
        return true;
    }

    private boolean checkListOfPaths(StringBuilder existList, StringBuilder nonExistList, Configuration conf)
            throws IOException {

        log.info("[" + actionId + "]::ActionInputCheck:: In checkListOfPaths for: " + nonExistList.toString());

        String[] uriList = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR);
        nonExistList.delete(0, nonExistList.length());
        boolean allExists = true;
        String existSeparator = "", nonExistSeparator = "";
        for (int i = 0; i < uriList.length; i++) {
            if (allExists) {
                allExists = pathExists(uriList[i], conf);
                log.info("[" + actionId + "]::ActionInputCheck:: File:" + uriList[i] + ", Exists? :" + allExists);
            }
            if (allExists) {
                existList.append(existSeparator).append(uriList[i]);
                existSeparator = CoordELFunctions.INSTANCE_SEPARATOR;
            }
            else {
                nonExistList.append(nonExistSeparator).append(uriList[i]);
                nonExistSeparator = CoordELFunctions.INSTANCE_SEPARATOR;
            }
        }
        return allExists;
    }

    private boolean pathExists(String sPath, Configuration actionConf) throws IOException {
        log.debug("checking for the file " + sPath);
        Path path = new Path(sPath);
        String user = ParamChecker.notEmpty(actionConf.get(OozieClient.USER_NAME), OozieClient.USER_NAME);
        String group = ParamChecker.notEmpty(actionConf.get(OozieClient.GROUP_NAME), OozieClient.GROUP_NAME);
        try {
            return Services.get().get(HadoopAccessorService.class).createFileSystem(user, group, path.toUri(),
                    new Configuration()).exists(path);
        }
        catch (HadoopAccessorException e) {
            throw new IOException(e);
        }
    }

    /**
     * The function create a list of URIs separated by "," using the instances time stamp and URI-template
     *
     * @param event : <data-in> event
     * @param instances : List of time stamp seprated by ","
     * @param unresolvedInstances : list of instance with latest/future function
     * @return : list of URIs separated by ",".
     * @throws Exception
     */
    private String createURIs(Element event, String instances, StringBuilder unresolvedInstances) throws Exception {
        if (instances == null || instances.length() == 0) {
            return "";
        }
        String[] instanceList = instances.split(CoordELFunctions.INSTANCE_SEPARATOR);
        StringBuilder uris = new StringBuilder();

        for (int i = 0; i < instanceList.length; i++) {
            int funcType = CoordCommandUtils.getFuncType(instanceList[i]);
            if (funcType == CoordCommandUtils.LATEST || funcType == CoordCommandUtils.FUTURE) {
                if (unresolvedInstances.length() > 0) {
                    unresolvedInstances.append(CoordELFunctions.INSTANCE_SEPARATOR);
                }
                unresolvedInstances.append(instanceList[i]);
                continue;
            }
            ELEvaluator eval = CoordELEvaluator.createURIELEvaluator(instanceList[i]);
            // uris.append(eval.evaluate(event.getChild("dataset",
            // event.getNamespace()).getChild("uri-template",
            // event.getNamespace()).getTextTrim(), String.class));
            if (uris.length() > 0) {
                uris.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            uris.append(CoordELFunctions.evalAndWrap(eval, event.getChild("dataset", event.getNamespace()).getChild(
                    "uri-template", event.getNamespace()).getTextTrim()));
        }
        return uris.toString();
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        new Services().init();
        String actionId = "0000000-091221141623042-oozie-dani-C@4";
        try {
            new CoordActionInputCheckXCommand(actionId).call();
            Thread.sleep(10000);
        }
        finally {
            new Services().destroy();
        }
    }

    @Override
    protected String getEntityKey() {
        return coordAction.getJobId();
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }
        try {
            coordAction = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
        LogUtils.setLogInfo(coordAction, logInfo);
    }
    
    @Override
    protected void loadState() throws CommandException {

    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        // this action should only get processed if current time >
        // materialization time
        // otherwise, requeue this action after 30 seconds
        Date nominalTime = coordAction.getNominalTime();
        Date currentTime = new Date();
        if (nominalTime.compareTo(currentTime) > 0) {
            queue(new CoordActionInputCheckXCommand(coordAction.getId()), Math.max(
                    (nominalTime.getTime() - currentTime.getTime()), COMMAND_REQUEUE_INTERVAL));
            // update lastModifiedTime
            coordAction.setLastModifiedTime(new Date());
            try {
                jpaService.execute(new org.apache.oozie.executor.jpa.CoordActionUpdateJPAExecutor(coordAction));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::ActionInputCheck:: nominal Time is newer than current time, so requeue and wait. Current="
                    + currentTime + ", nominal=" + nominalTime);
        }
        
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId + "]::ActionInputCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }
    }

}
