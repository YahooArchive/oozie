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

import java.sql.Timestamp;
import java.util.Date;
import java.util.Set;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowActionGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.WorkflowJobUpdateJPAExecutor;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.LiteWorkflowStoreService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.LogUtils;
import org.apache.oozie.util.XLog;

/**
 * Executes the check command for ActionHandlers. </p> Ensures the action is in
 * RUNNING state before executing
 * {@link ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckXCommand extends ActionXCommand<Void> {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";
    private String actionId;
    private String jobId;
    private int actionCheckDelay;
    private WorkflowJobBean wfJob = null;
    private WorkflowActionBean wfAction = null;
    private JPAService jpaService = null;
    private ActionExecutor executor = null;

    public ActionCheckXCommand(String actionId) {
        this(actionId, -1);
    }

    public ActionCheckXCommand(String actionId, int priority, int checkDelay) {
        super("action.check", "action.check", priority);
        this.actionId = actionId;
        this.actionCheckDelay = checkDelay;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    public ActionCheckXCommand(String actionId, int checkDelay) {
        this(actionId, 0, checkDelay);
    }

    @Override
    protected void eagerLoadState() throws CommandException {
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
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (wfJob == null) {
            throw new PreconditionException(ErrorCode.E0604, jobId);
        }
        if (wfAction == null) {
            throw new PreconditionException(ErrorCode.E0605, actionId);
        }
        // if the action has been updated, quit this command
        if (actionCheckDelay > 0) {
            Timestamp actionCheckTs = new Timestamp(System.currentTimeMillis() - actionCheckDelay * 1000);
            Timestamp actionLmt = wfAction.getLastCheckTimestamp();
            if (actionLmt.after(actionCheckTs)) {
                throw new PreconditionException(ErrorCode.E0817, actionId);
            }
        }

        executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor == null) {
            throw new CommandException(ErrorCode.E0802, wfAction.getType());
        }
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
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (!wfAction.isPending() || wfAction.getStatus() != WorkflowActionBean.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0815, wfAction.getPending(), wfAction.getStatusStr());
        }
        if (wfJob.getStatus() != WorkflowJob.Status.RUNNING) {
            wfAction.setLastCheckTime(new Date());
            try {
                jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            throw new PreconditionException(ErrorCode.E0818, wfAction.getId(), wfJob.getId(), wfJob.getStatus());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED ActionCheckXCommand for wf actionId=" + actionId + " priority =" + getPriority());

        ActionExecutorContext context = null;
        try {
            boolean isRetry = false;
            boolean isUserRetry = false;
            context = new ActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry, isUserRetry);
            incrActionCounter(wfAction.getType(), 1);

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.check(context, wfAction);
            cron.stop();
            addActionCron(wfAction.getType(), cron);

            if (wfAction.isExecutionComplete()) {
                if (!context.isExecuted()) {
                    LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()", executor
                            .getType());
                    wfAction.setErrorInfo(EXEC_DATA_MISSING,
                            "Execution Complete, but Execution Data Missing from Action");
                    failJob(context);
                    wfAction.setLastCheckTime(new Date());
                    jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
                    jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
                    return null;
                }
                wfAction.setPending();
                queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
            }
            wfAction.setLastCheckTime(new Date());
            jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
            jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
        }
        catch (ActionExecutorException ex) {
            LOG.warn("Exception while executing check(). Error Code [{0}], Message[{1}]", ex.getErrorCode(), ex
                    .getMessage(), ex);

            switch (ex.getErrorType()) {
                case FAILED:
                    failAction(wfJob, wfAction);
                    break;
            }
            wfAction.setLastCheckTime(new Date());
            try {
                jpaService.execute(new WorkflowActionUpdateJPAExecutor(wfAction));
                jpaService.execute(new WorkflowJobUpdateJPAExecutor(wfJob));
            }
            catch (JPAExecutorException e) {
                throw new CommandException(e);
            }
            return null;
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }

        LOG.debug("ENDED ActionCheckXCommand for wf actionId=" + actionId + ", jobId=" + jobId);
        return null;
    }

    private void failAction(WorkflowJobBean workflow, WorkflowActionBean action) throws CommandException {
        String errorCode = action.getErrorCode();
        Set<String> allowedRetryCode = LiteWorkflowStoreService.getUserRetryErrorCode();

        if (allowedRetryCode.contains(errorCode) && action.getUserRetryCount() < action.getUserRetryMax()) {
            LOG.info("Preparing retry this action [{0}], errorCode [{1}], userRetryCount [{2}], "
                    + "userRetryMax [{3}], userRetryInterval [{4}]", action.getId(), errorCode, action
                    .getUserRetryCount(), action.getUserRetryMax(), action.getUserRetryInterval());
            int interval = action.getUserRetryInterval() * 60 * 1000;
            action.setStatus(WorkflowAction.Status.USER_RETRY);
            action.incrmentUserRetryCount();
            action.setPending();
            queue(new ActionStartXCommand(action.getId(), action.getType()), interval);
        }
        else {
            LOG.warn("Failing Job [{0}] due to failed action [{1}]", workflow.getId(), action.getId());
            action.resetPending();
            action.setStatus(Status.FAILED);
            workflow.setStatus(WorkflowJob.Status.FAILED);
            InstrumentUtils.incrJobCounter(INSTR_FAILED_JOBS_COUNTER, 1, getInstrumentation());
        }
    }
}
