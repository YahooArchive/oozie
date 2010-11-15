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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowAction.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.XLog;

/**
 * Executes the check command for ActionHandlers. </p> Ensures the action is in
 * RUNNING state before executing
 * {@link ActionExecutor#check(org.apache.oozie.action.ActionExecutor.Context, org.apache.oozie.client.WorkflowAction)}
 */
public class ActionCheckXCommand extends WorkflowActionXCommand<Void> {
    public static final String EXEC_DATA_MISSING = "EXEC_DATA_MISSING";
    private final XLog LOG = XLog.getLog(getClass());
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
                this.wfJob = jpaService.execute(new WorkflowJobGetCommand(jobId));
                this.wfAction = jpaService.execute(new WorkflowActionGetCommand(actionId));
                setLogInfo(wfJob);
                setLogInfo(wfAction);
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
            jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
            throw new PreconditionException(ErrorCode.E0818, wfAction.getId(), wfJob.getId(), wfJob.getStatus());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED ActionCheckXCommand for wf actionId=" + actionId + " priority =" + getPriority());

        ActionExecutorContext context = null;
        try {
            boolean isRetry = false;
            context = new WorkflowActionXCommand.ActionExecutorContext(wfJob, wfAction, isRetry);
            incrActionCounter(wfAction.getType(), 1);

            Instrumentation.Cron cron = new Instrumentation.Cron();
            cron.start();
            executor.check(context, wfAction);
            cron.stop();
            addActionCron(wfAction.getType(), cron);

            if (wfAction.isExecutionComplete()) {
                if (!context.isExecuted()) {
                    LOG.warn(XLog.OPS, "Action Completed, ActionExecutor [{0}] must call setExecutionData()",
                            executor.getType());
                    wfAction.setErrorInfo(EXEC_DATA_MISSING,
                    "Execution Complete, but Execution Data Missing from Action");
                    failJob(context);
                    wfAction.setLastCheckTime(new Date());
                    jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
                    jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
                    return null;
                }
                wfAction.setPending();
                queue(new ActionEndXCommand(wfAction.getId(), wfAction.getType()));
            }
            wfAction.setLastCheckTime(new Date());
            jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
            jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
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
            jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
            jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
            return null;
        }

        LOG.debug("ENDED ActionCheckXCommand for wf actionId=" + actionId + ", jobId=" + jobId);
        return null;
    }

    private void failAction(WorkflowJobBean workflow, WorkflowActionBean action) throws CommandException {
        LOG.warn("Failing Job [{0}] due to failed action [{1}]", workflow.getId(), action.getId());
        action.resetPending();
        action.setStatus(Status.FAILED);
        workflow.setStatus(WorkflowJob.Status.FAILED);
        incrJobCounter(INSTR_FAILED_JOBS_COUNTER, 1);
    }
}
