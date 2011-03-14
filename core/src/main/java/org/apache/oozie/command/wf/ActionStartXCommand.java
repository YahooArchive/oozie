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

import java.util.Date;

import javax.servlet.jsp.el.ELException;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.FaultInjection;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.coord.CoordActionUpdateXCommand;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.ELEvaluationException;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.XmlUtils;
import org.apache.oozie.util.db.SLADbXOperations;

public class ActionStartXCommand extends ActionXCommand<Void> {
    public static final String EL_ERROR = "EL_ERROR";
    public static final String EL_EVAL_ERROR = "EL_EVAL_ERROR";
    public static final String COULD_NOT_START = "COULD_NOT_START";
    public static final String START_DATA_MISSING = "START_DATA_MISSING";
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";

    private final XLog LOG = XLog.getLog(getClass());
    private String jobId = null;
    private String actionId = null;
    private WorkflowJobBean wfJob = null;
    private WorkflowActionBean wfAction = null;
    private JPAService jpaService = null;
    private ActionExecutor executor = null;

    public ActionStartXCommand(String actionId, String type) {
        super("action.start", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
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
                this.wfJob = jpaService.execute(new WorkflowJobGetJPAExecutor(jobId));
                this.wfAction = jpaService.execute(new WorkflowActionGetJPAExecutor(actionId));
                LogUtils.setLogInfo(wfJob, logInfo);
                LogUtils.setLogInfo(wfAction, logInfo);
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
        if (wfJob == null) {
            throw new PreconditionException(ErrorCode.E0604, jobId);
        }
        if (wfAction == null) {
            throw new PreconditionException(ErrorCode.E0605, actionId);
        }
        if (wfAction.isPending()
                && (wfAction.getStatus() == WorkflowActionBean.Status.PREP
                        || wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL)) {
            if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
                throw new PreconditionException(ErrorCode.E0810, WorkflowJob.Status.RUNNING.toString());
            }
        }
        else {
            throw new PreconditionException(ErrorCode.E0816, wfAction.getPending(), wfAction.getStatusStr());
        }

        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
        }
    }

    @Override
    protected Void execute() throws CommandException {

        LOG.debug("STARTED ActionStartXCommand for wf actionId=" + actionId);
        Configuration conf = wfJob.getWorkflowInstance().getConf();

        int maxRetries = conf.getInt(OozieClient.ACTION_MAX_RETRIES, executor.getMaxRetries());
        long retryInterval = conf.getLong(OozieClient.ACTION_RETRY_INTERVAL, executor.getRetryInterval());
        executor.setMaxRetries(maxRetries);
        executor.setRetryInterval(retryInterval);

        ActionExecutorContext context = null;
        try {
            boolean isRetry = false;
            if (wfAction.getStatus() == WorkflowActionBean.Status.START_RETRY
                    || wfAction.getStatus() == WorkflowActionBean.Status.START_MANUAL) {
                isRetry = true;
            }
            context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry);
            try {
                String tmpActionConf = XmlUtils.removeComments(wfAction.getConf());
                String actionConf = context.getELEvaluator().evaluate(tmpActionConf, String.class);
                wfAction.setConf(actionConf);

                LOG.debug("Start, name [{0}] type [{1}] configuration{E}{E}{2}{E}", wfAction.getName(), wfAction
                        .getType(), actionConf);

            }
            catch (ELEvaluationException ex) {
                throw new ActionExecutorException(ActionExecutorException.ErrorType.TRANSIENT, EL_EVAL_ERROR, ex
                        .getMessage(), ex);
            }
            catch (ELException ex) {
                context.setErrorInfo(EL_ERROR, ex.getMessage());
                LOG.warn("ELException in ActionStartXCommand ", ex.getMessage(), ex);
                handleError(context, wfJob, wfAction);
                return null;
            }
            catch (org.jdom.JDOMException je) {
                context.setErrorInfo("ParsingError", je.getMessage());
                LOG.warn("JDOMException in ActionStartXCommand ", je.getMessage(), je);
                handleError(context, wfJob, wfAction);
                return null;
            }
            catch (Exception ex) {
                context.setErrorInfo(EL_ERROR, ex.getMessage());
                LOG.warn("Exception in ActionStartXCommand ", ex.getMessage(), ex);
                handleError(context, wfJob, wfAction);
                return null;
            }
            wfAction.setErrorInfo(null, null);
            incrActionCounter(wfAction.getType(), 1);

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.start(context, wfAction);
            cron.stop();
            FaultInjection.activate("org.apache.oozie.command.SkipCommitFaultInjection");
            addActionCron(wfAction.getType(), cron);

            wfAction.setRetries(0);
            if (wfAction.isExecutionComplete()) {
                if (!context.isExecuted()) {
                    LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                            .getType());
                    wfAction.setErrorInfo(EXEC_DATA_MISSING,
                            "Execution Complete, but Execution Data Missing from Action");
                    failJob(context);
                    jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
                    jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
                    return null;
                }
                wfAction.setPending();
                queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
            }
            else {
                if (!context.isStarted()) {
                    LOG.warn(XLog.OPS, "Action Started, ActionExecutor [{0}] must call setStartData()", executor
                            .getType());
                    wfAction.setErrorInfo(START_DATA_MISSING, "Execution Started, but Start Data Missing from Action");
                    failJob(context);
                    jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
                    jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
                    return null;
                }
                queue(new NotificationXCommand(wfJob, wfAction));
            }

            LOG.warn(XLog.STD, "[***" + wfAction.getId() + "***]" + "Action status=" + wfAction.getStatusStr());

            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
            jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
            // Add SLA status event (STARTED) for WF_ACTION
            SLADbXOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.STARTED,
                    SlaAppType.WORKFLOW_ACTION);
            LOG.warn(XLog.STD, "[***" + wfAction.getId() + "***]" + "Action updated in DB!");

        }
        catch (ActionExecutorException ex) {
            LOG.warn("Error starting action [{0}]. ErrorType [{1}], ErrorCode [{2}], Message [{3}]",
                    wfAction.getName(), ex.getErrorType(), ex.getErrorCode(), ex.getMessage(), ex);
            wfAction.setErrorInfo(ex.getErrorCode(), ex.getMessage());
            switch (ex.getErrorType()) {
                case TRANSIENT:
                    if (!handleTransient(context, executor, WorkflowAction.Status.START_RETRY)) {
                        handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                        wfAction.setPendingAge(new Date());
                        wfAction.setRetries(0);
                        wfAction.setStartTime(null);
                    }
                    break;
                case NON_TRANSIENT:
                    handleNonTransient(context, executor, WorkflowAction.Status.START_MANUAL);
                    break;
                case ERROR:
                    handleError(context, executor, WorkflowAction.Status.ERROR.toString(), true,
                            WorkflowAction.Status.DONE);
                    break;
                case FAILED:
                    try {
                        failJob(context);
                        // update coordinator action
                        new CoordActionUpdateXCommand(wfJob).call();
                        new WfEndXCommand(wfJob).call(); //To delete the WF temp dir
                        SLADbXOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED,
                                SlaAppType.WORKFLOW_ACTION);
                        SLADbXOperations.writeStausEvent(wfJob.getSlaXml(), wfJob.getId(), Status.FAILED,
                                SlaAppType.WORKFLOW_JOB);
                    }
                    catch (XException x) {
                        LOG.warn("ActionStartXCommand - case:FAILED ", x.getMessage());
                    }
                    break;
            }
            try {
                jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
                jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
            }
            catch (JPAExecutorException je) {
                throw new CommandException(je);
            }
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }

        LOG.debug("ENDED ActionStartXCommand for wf actionId=" + actionId + ", jobId=" + jobId);

        return null;
    }

    private void handleError(ActionExecutorContext context, WorkflowJobBean workflow, WorkflowActionBean action)
            throws CommandException {
        failJob(context);
        try {
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(action));
            jpaService.execute(new WorkflowJobUpdateJPAExecutor(workflow));
        }
        catch (JPAExecutorException je) {
            throw new CommandException(je);
        }
        SLADbXOperations.writeStausEvent(action.getSlaXml(), action.getId(), Status.FAILED, SlaAppType.WORKFLOW_ACTION);
        SLADbXOperations.writeStausEvent(workflow.getSlaXml(), workflow.getId(), Status.FAILED, SlaAppType.WORKFLOW_JOB);
        // update coordinator action
        new CoordActionUpdateXCommand(workflow).call();
        new WfEndXCommand(wfJob).call(); //To delete the WF temp dir
        return;
    }

}
