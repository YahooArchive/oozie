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
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.Job;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.coord.CoordELEvaluator;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.executor.jpa.CoordActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.CoordJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
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
import org.apache.oozie.util.XmlUtils;
import org.jdom.Element;

/**
 * The command to check if an action's data input paths exist in the file system.
 */
public class CoordActionInputCheckXCommand extends CoordinatorXCommand<Void> {

    private final String actionId;
    private final int COMMAND_REQUEUE_INTERVAL = 60000; // 1 minute
    private CoordinatorActionBean coordAction = null;
    private CoordinatorJobBean coordJob = null;
    private JPAService jpaService = null;

    public CoordActionInputCheckXCommand(String actionId) {
        super("coord_action_input", "coord_action_input", 1);
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        LOG.info("[" + actionId + "]::ActionInputCheck:: Action is in WAITING state.");

        // this action should only get processed if current time > nominal time;
        // otherwise, requeue this action for delay execution;
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
            LOG.info("[" + actionId
                    + "]::ActionInputCheck:: nominal Time is newer than current time, so requeue and wait. Current="
                    + currentTime + ", nominal=" + nominalTime);

            return null;
        }

        StringBuilder actionXml = new StringBuilder(coordAction.getActionXml());
        Instrumentation.Cron cron = new Instrumentation.Cron();
        try {
            Configuration actionConf = new XConfiguration(new StringReader(coordAction.getRunConf()));
            cron.start();
            StringBuilder existList = new StringBuilder();
            StringBuilder nonExistList = new StringBuilder();
            StringBuilder nonResolvedList = new StringBuilder();
            CoordCommandUtils.getResolvedList(coordAction.getMissingDependencies(), nonExistList, nonResolvedList);

            LOG.info("[" + actionId + "]::CoordActionInputCheck:: Missing deps:" + nonExistList.toString() + " "
                    + nonResolvedList.toString());
            Date actualTime = new Date();
            boolean status = checkInput(actionXml, existList, nonExistList, actionConf, actualTime);
            coordAction.setLastModifiedTime(actualTime);
            coordAction.setActionXml(actionXml.toString());
            if (nonResolvedList.length() > 0 && status == false) {
                nonExistList.append(CoordCommandUtils.RESOLVED_UNRESOLVED_SEPARATOR).append(nonResolvedList);
            }
            coordAction.setMissingDependencies(nonExistList.toString());
            if (status == true) {
                coordAction.setStatus(CoordinatorAction.Status.READY);
                // pass jobID to the CoordActionReadyXCommand
                queue(new CoordActionReadyXCommand(coordAction.getJobId()), 100);
            }
            else {
                long waitingTime = (actualTime.getTime() - Math.max(coordAction.getNominalTime().getTime(), coordAction
                        .getCreatedTime().getTime()))
                        / (60 * 1000);
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

    /**
     * To check the list of input paths if all of them exist
     *
     * @param actionXml action xml
     * @param existList the list of existed paths
     * @param nonExistList the list of non existed paths
     * @param conf action configuration
     * @param actualTime current time
     * @return true if all input paths are existed
     * @throws Exception thrown of unable to check input path
     */
    protected boolean checkInput(StringBuilder actionXml, StringBuilder existList, StringBuilder nonExistList,
            Configuration conf, Date actualTime) throws Exception {
        Element eAction = XmlUtils.parseXml(actionXml.toString());
        boolean allExist = checkResolvedUris(eAction, existList, nonExistList, conf);
        if (allExist) {
            LOG.debug("[" + actionId + "]::ActionInputCheck:: Checking Latest/future");
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
     * @param eAction action element
     * @param conf action configuration
     * @throws Exception thrown if failed to resolve data properties
     * @update modify 'Action' element with appropriate list of files.
     */
    @SuppressWarnings("unchecked")
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

    /**
     * To resolve property value which contains el functions
     *
     * @param tagName tag name
     * @param elem the child element of "property" element
     * @param eval el functions evaluator
     * @throws Exception thrown if unable to resolve tag value
     */
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
            LOG.warn(" Value NOT FOUND " + tagName);
        }
    }

    /**
     * Check if any unsolved paths under data output. Resolve the unresolved data input paths.
     *
     * @param eAction action element
     * @param actionConf action configuration
     * @param actualTime current time
     * @return true if successful to resolve input and output paths
     * @throws Exception thrown if failed to resolve data input and output paths
     */
    @SuppressWarnings("unchecked")
    private boolean checkUnresolvedInstances(Element eAction, Configuration actionConf, Date actualTime)
            throws Exception {
        String strAction = XmlUtils.prettyPrint(eAction).toString();
        Date nominalTime = DateUtils.parseDateUTC(eAction.getAttributeValue("action-nominal-time"));
        StringBuffer resultedXml = new StringBuffer();

        boolean ret;
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            ret = materializeUnresolvedEvent(inputList.getChildren("data-in", eAction.getNamespace()), nominalTime,
                    actualTime, actionConf);
            if (ret == false) {
                resultedXml.append(strAction);
                return false;
            }
        }

        // Using latest() or future() in output-event is not intuitive.
        // We need to make sure, this assumption is correct.
        Element outputList = eAction.getChild("output-events", eAction.getNamespace());
        if (outputList != null) {
            for (Element dEvent : (List<Element>) outputList.getChildren("data-out", eAction.getNamespace())) {
                if (dEvent.getChild("unresolved-instances", dEvent.getNamespace()) != null) {
                    throw new CommandException(ErrorCode.E1006, "coord:latest()/future()",
                            " not permitted in output-event ");
                }
            }
        }
        return true;
    }

    /**
     * Resolve the list of data input paths
     *
     * @param eDataEvents the list of data input elements
     * @param nominalTime action nominal time
     * @param actualTime current time
     * @param conf action configuration
     * @return true if all unresolved URIs can be resolved
     * @throws Exception thrown if failed to resolve data input paths
     */
    @SuppressWarnings("unchecked")
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
                    LOG.info("[" + actionId + "]::Cannot resolve: " + ret);
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

    /**
     * Check all resolved URIs existence
     *
     * @param eAction action element
     * @param existList the list of existed paths
     * @param nonExistList the list of paths to check existence
     * @param conf action configuration
     * @return true if all nonExistList paths exist
     * @throws IOException thrown if unable to access the path
     */
    private boolean checkResolvedUris(Element eAction, StringBuilder existList, StringBuilder nonExistList,
            Configuration conf) throws IOException {
        LOG.info("[" + actionId + "]::ActionInputCheck:: In checkResolvedUris...");
        Element inputList = eAction.getChild("input-events", eAction.getNamespace());
        if (inputList != null) {
            if (nonExistList.length() > 0) {
                checkListOfPaths(existList, nonExistList, conf);
            }
            return nonExistList.length() == 0;
        }
        return true;
    }

    /**
     * Check a list of non existed paths and add to exist list if it exists
     *
     * @param existList the list of existed paths
     * @param nonExistList the list of paths to check existence
     * @param conf action configuration
     * @return true if all nonExistList paths exist
     * @throws IOException thrown if unable to access the path
     */
    private boolean checkListOfPaths(StringBuilder existList, StringBuilder nonExistList, Configuration conf)
            throws IOException {

        String[] uriList = nonExistList.toString().split(CoordELFunctions.INSTANCE_SEPARATOR);
        if (uriList[0] != null) {
            LOG.info("[" + actionId + "]::ActionInputCheck:: In checkListOfPaths: " + uriList[0] + " is Missing.");
        }

        nonExistList.delete(0, nonExistList.length());
        boolean allExists = true;
        String existSeparator = "", nonExistSeparator = "";
        for (int i = 0; i < uriList.length; i++) {
            if (allExists) {
                allExists = pathExists(uriList[i], conf);
                LOG.info("[" + actionId + "]::ActionInputCheck:: File:" + uriList[i] + ", Exists? :" + allExists);
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

    /**
     * Check if given path exists
     *
     * @param sPath uri path
     * @param actionConf action configuration
     * @return true if path exists
     * @throws IOException thrown if unable to access the path
     */
    private boolean pathExists(String sPath, Configuration actionConf) throws IOException {
        LOG.debug("checking for the file " + sPath);
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
     * @throws Exception thrown if failed to create URIs from unresolvedInstances
     */
    @SuppressWarnings("unused")
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
            if (uris.length() > 0) {
                uris.append(CoordELFunctions.INSTANCE_SEPARATOR);
            }
            uris.append(CoordELFunctions.evalAndWrap(eval, event.getChild("dataset", event.getNamespace()).getChild(
                    "uri-template", event.getNamespace()).getTextTrim()));
        }
        return uris.toString();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return coordAction.getJobId();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        loadState();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {
        if (jpaService == null) {
            jpaService = Services.get().get(JPAService.class);
        }
        try {
            coordAction = jpaService.execute(new CoordActionGetJPAExecutor(actionId));
            coordJob = jpaService.execute(new CoordJobGetJPAExecutor(coordAction.getJobId()));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        LogUtils.setLogInfo(coordAction, logInfo);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (coordAction.getStatus() != CoordinatorActionBean.Status.WAITING) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionInputCheck:: Ignoring action. Should be in WAITING state, but state="
                    + coordAction.getStatus());
        }
        if (coordJob.getStatus() != Job.Status.RUNNING && coordJob.getStatus() != Job.Status.PAUSED && coordJob.getStatus() != Job.Status.PAUSEDWITHERROR) {
            throw new PreconditionException(ErrorCode.E1100, "[" + actionId
                    + "]::CoordActionInputCheck:: Ignoring action. Coordinator job is not in RUNNING state, but state="
                    + coordJob.getStatus());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getKey()
     */
    @Override
    public String getKey(){
        return getName() + "_" + actionId;
    }

}
