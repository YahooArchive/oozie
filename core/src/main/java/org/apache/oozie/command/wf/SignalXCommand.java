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
package org.apache.oozie.command.wf;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.CoordinatorAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordActionReadyCommand;
import org.apache.oozie.command.coord.CoordActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionInsertCommand;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.CoordinatorJobException;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.SchemaService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.StoreService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.WorkflowStoreService;
import org.apache.oozie.store.CoordinatorStore;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.util.ELEvaluator;
import org.apache.oozie.util.XConfiguration;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbXOperations;
import org.apache.openjpa.lib.log.Log;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;

import java.io.StringReader;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SignalXCommand extends WorkflowXCommand<Void> {

    protected static final String INSTR_SUCCEEDED_JOBS_COUNTER_NAME = "succeeded";

    private final XLog LOG = XLog.getLog(getClass());
    private JPAService jpaService = null;
    private String jobId;
    private String actionId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;

    protected SignalXCommand(String name, int priority, String jobId) {
        super(name, name, priority);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
    }

    public SignalXCommand(String jobId, String actionId) {
        super("signal", "signal", 1);
        this.jobId = ParamChecker.notEmpty(jobId, "jobId");
        this.actionId = ParamChecker.notEmpty(actionId, "actionId");
    }

    @Override
    protected boolean isLockRequired() {
        return true;
    }

    @Override
    protected String getEntityKey() {
        return this.jobId;
    }

    @Override
    protected void loadState() throws CommandException {
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJob = jpaService.execute(new WorkflowJobGetCommand(jobId));
                setLogInfo(wfJob);
                if (actionId != null) {
                    this.wfAction = jpaService.execute(new WorkflowActionGetCommand(actionId));
                    setLogInfo(wfAction);
                }
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if ((wfAction == null) || (wfAction.isComplete() && wfAction.isPending())) {
            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING
                    && wfJob.getStatus() != WorkflowJob.Status.PREP) {
                throw new PreconditionException(ErrorCode.E0813, wfJob.getStatusStr());
            }
        } else {
            throw new PreconditionException(ErrorCode.E0814, actionId, wfAction.getStatusStr(), wfAction.isPending());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
        WorkflowInstance workflowInstance = wfJob.getWorkflowInstance();
        workflowInstance.setTransientVar(WorkflowStoreService.WORKFLOW_BEAN, wfJob);
        boolean completed = false;
        boolean skipAction = false;
        if (wfAction == null) {
            if (wfJob.getStatus() == WorkflowJob.Status.PREP) {
                completed = workflowInstance.start();
                wfJob.setStatus(WorkflowJob.Status.RUNNING);
                wfJob.setStartTime(new Date());
                wfJob.setWorkflowInstance(workflowInstance);
                // 1. Add SLA status event for WF-JOB with status STARTED
                // 2. Add SLA registration events for all WF_ACTIONS
                SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), jobId, Status.STARTED, SlaAppType.WORKFLOW_JOB);
                writeSLARegistrationForAllActions(workflowInstance.getApp().getDefinition(), wfJob
                        .getUser(), wfJob.getGroup(), wfJob.getConf());
                queue(new NotificationXCommand(wfJob));
            }
            else {
                throw new CommandException(ErrorCode.E0801, wfJob.getId());
            }
        }
        else {
            String skipVar = workflowInstance.getVar(wfAction.getName() + WorkflowInstance.NODE_VAR_SEPARATOR
                    + ReRunCommand.TO_SKIP);
            if (skipVar != null) {
                skipAction = skipVar.equals("true");
            }
            completed = workflowInstance.signal(wfAction.getExecutionPath(), wfAction.getSignalValue());
            wfJob.setWorkflowInstance(workflowInstance);
            wfAction.resetPending();
            if (!skipAction) {
                wfAction.setTransition(workflowInstance.getTransition(wfAction.getName()));
            }
            jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
        }

        if (completed) {
            for (String actionToKillId : WorkflowStoreService.getActionsToKill(workflowInstance)) {
                WorkflowActionBean actionToKill = jpaService.execute(new WorkflowActionGetCommand(actionToKillId));
                actionToKill.setPending();
                actionToKill.setStatus(WorkflowActionBean.Status.KILLED);
                jpaService.execute(new WorkflowActionUpdateCommand(actionToKill));
                queue(new WorkflowActionKillXCommand(actionToKill.getId(), actionToKill.getType()));
            }

            for (String actionToFailId : WorkflowStoreService.getActionsToFail(workflowInstance)) {
                WorkflowActionBean actionToFail = jpaService.execute(new WorkflowActionGetCommand(actionToFailId));
                actionToFail.resetPending();
                actionToFail.setStatus(WorkflowActionBean.Status.FAILED);
                SLADbXOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED, SlaAppType.WORKFLOW_ACTION);
                jpaService.execute(new WorkflowActionUpdateCommand(actionToFail));
            }

            wfJob.setStatus(WorkflowJob.Status.valueOf(workflowInstance.getStatus().toString()));
            wfJob.setEndTime(new Date());
            wfJob.setWorkflowInstance(workflowInstance);
            Status slaStatus = Status.SUCCEEDED;
            switch (wfJob.getStatus()) {
                case SUCCEEDED:
                    slaStatus = Status.SUCCEEDED;
                    break;
                case KILLED:
                    slaStatus = Status.KILLED;
                    break;
                case FAILED:
                    slaStatus = Status.FAILED;
                    break;
                default: // TODO SUSPENDED
                    break;
            }
            SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), jobId, slaStatus, SlaAppType.WORKFLOW_JOB);
            queue(new NotificationXCommand(wfJob));
            if (wfJob.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                incrJobCounter(INSTR_SUCCEEDED_JOBS_COUNTER_NAME, 1);
            }
        }
        else {
            for (WorkflowActionBean newAction : WorkflowStoreService.getStartedActions(workflowInstance)) {
                String skipVar = workflowInstance.getVar(newAction.getName()
                        + WorkflowInstance.NODE_VAR_SEPARATOR + ReRunCommand.TO_SKIP);
                boolean skipNewAction = false;
                if (skipVar != null) {
                    skipNewAction = skipVar.equals("true");
                }
                if (skipNewAction) {
                    WorkflowActionBean oldAction = jpaService.execute(new WorkflowActionGetCommand(newAction.getId()));
                    oldAction.setPending();
                    jpaService.execute(new WorkflowActionUpdateCommand(oldAction));

                    queue(new SignalXCommand(jobId, oldAction.getId()));
                }
                else {
                    newAction.setPending();
                    String actionSlaXml = getActionSLAXml(newAction.getName(), workflowInstance.getApp()
                            .getDefinition(), wfJob.getConf());
                    newAction.setSlaXml(actionSlaXml);
                    jpaService.execute(new WorkflowActionInsertCommand(newAction));
                    queue(new ActionStartXCommand(newAction.getId(), newAction.getType()));
                }
            }
        }

        jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
        XLog.getLog(getClass()).debug("Updated the workflow status to " + wfJob.getId() + "  status ="+ wfJob.getStatusStr());
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING
                && wfJob.getStatus() != WorkflowJob.Status.SUSPENDED) {
            queue(new CoordActionUpdateXCommand(wfJob));
        }
        LOG.debug("ENDED SignalCommand for jobid=" + jobId + ", actionId=" + actionId);
    }

    public static ELEvaluator createELEvaluatorForGroup(Configuration conf, String group) {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator(group);
        for (Map.Entry<String, String> entry : conf) {
            eval.setVariable(entry.getKey(), entry.getValue());
        }
        return eval;
    }

    @SuppressWarnings("unchecked")
    private String getActionSLAXml(String actionName, String wfXml, String wfConf) throws CommandException {
        String slaXml = null;
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                if (action.getAttributeValue("name").equals(actionName) == false) {
                    continue;
                }
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    slaXml = XmlUtils.prettyPrint(eSla).toString();
                    break;
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, e.getMessage(), e);
        }
        return slaXml;
    }

    private String resolveSla(Element eSla, Configuration conf) throws CommandException {
        String slaXml = null;
        try {
            ELEvaluator evalSla = SubmitCommand.createELEvaluatorForGroup(conf, "wf-sla-submit");
            slaXml = SubmitCommand.resolveSla(eSla, evalSla);
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1004, e.getMessage(), e);
        }
        return slaXml;
    }

    @SuppressWarnings("unchecked")
    private void writeSLARegistrationForAllActions(String wfXml, String user, String group, String strConf) throws CommandException {
        try {
            Element eWfJob = XmlUtils.parseXml(wfXml);
            Configuration conf = new XConfiguration(new StringReader(strConf));
            for (Element action : (List<Element>) eWfJob.getChildren("action", eWfJob.getNamespace())) {
                Element eSla = action.getChild("info", Namespace.getNamespace(SchemaService.SLA_NAME_SPACE_URI));
                if (eSla != null) {
                    String slaXml = resolveSla(eSla, conf);
                    eSla = XmlUtils.parseXml(slaXml);
                    String actionId = Services.get().get(UUIDService.class).generateChildId(jobId,
                                                                                            action.getAttributeValue("name") + "");
                    SLADbXOperations.writeSlaRegistrationEvent(eSla, actionId, SlaAppType.WORKFLOW_ACTION, user, group);
                }
            }
        }
        catch (Exception e) {
            throw new CommandException(ErrorCode.E1007, "workflow:Actions " + jobId, e);
        }

    }

}
