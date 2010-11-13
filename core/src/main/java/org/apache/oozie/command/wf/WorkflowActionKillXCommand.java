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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.XException;
import org.apache.oozie.client.SLAEvent.SlaAppType;
import org.apache.oozie.client.SLAEvent.Status;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.WorkflowActionGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.action.ActionExecutor;
import org.apache.oozie.action.ActionExecutorException;
import org.apache.oozie.service.ActionService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.UUIDService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.db.SLADbOperations;

/**
 * Kill workflow action and invoke action executor to kill the underlying context.
 *
 */
public class WorkflowActionKillXCommand extends WorkflowActionXCommand<Void> {
    private String actionId;
    private String jobId;
    private WorkflowJobBean wfJob;
    private WorkflowActionBean wfAction;
    private final XLog LOG = XLog.getLog(getClass());
    private JPAService jpaService = null;

    public WorkflowActionKillXCommand(String actionId, String type) {
        super("action.kill", type, 0);
        this.actionId = actionId;
        this.jobId = Services.get().get(UUIDService.class).getId(actionId);
    }

    public WorkflowActionKillXCommand(String actionId) {
        this(actionId, "action.kill");
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
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (!wfAction.isPending() || (wfAction.getStatus() != WorkflowActionBean.Status.KILLED)) {
            throw new PreconditionException(ErrorCode.E0726, wfAction.getId());
        }
    }

    @Override
    protected Void execute() throws CommandException {
        LOG.debug("STARTED WorkflowActionKillXCommand for action " + actionId);

        ActionExecutor executor = Services.get().get(ActionService.class).getExecutor(wfAction.getType());
        if (executor != null) {
            try {
                boolean isRetry = false;
                ActionExecutorContext context = new WorkflowActionXCommand.ActionExecutorContext(wfJob, wfAction,
                        isRetry);
                incrActionCounter(wfAction.getType(), 1);

                Instrumentation.Cron cron = new Instrumentation.Cron();
                cron.start();
                executor.kill(context, wfAction);
                cron.stop();
                addActionCron(wfAction.getType(), cron);

                wfAction.resetPending();
                wfAction.setStatus(WorkflowActionBean.Status.KILLED);

                jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
                jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
                // Add SLA status event (KILLED) for WF_ACTION
                SLADbOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.KILLED,
                        SlaAppType.WORKFLOW_ACTION);
                queue(new NotificationXCommand(wfJob, wfAction));
            }
            catch (ActionExecutorException ex) {
                wfAction.resetPending();
                wfAction.setStatus(WorkflowActionBean.Status.FAILED);
                wfAction.setErrorInfo(ex.getErrorCode().toString(),
                        "KILL COMMAND FAILED - exception while executing job kill");
                wfJob.setStatus(WorkflowJobBean.Status.KILLED);
                jpaService.execute(new WorkflowActionUpdateCommand(wfAction));
                jpaService.execute(new WorkflowJobUpdateCommand(wfJob));
                // What will happen to WF and COORD_ACTION, NOTIFICATION?
                SLADbOperations.writeStausEvent(wfAction.getSlaXml(), wfAction.getId(), Status.FAILED,
                        SlaAppType.WORKFLOW_ACTION);
                XLog.getLog(getClass()).warn("Exception while executing kill(). Error Code [{0}], Message[{1}]",
                        ex.getErrorCode(), ex.getMessage(), ex);
            }
        }
        LOG.debug("ENDED WorkflowActionKillXCommand for action " + actionId);
        return null;
    }

}
