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

import java.util.List;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.jpa.WorkflowActionRetryManualGetCommand;
import org.apache.oozie.command.jpa.WorkflowActionUpdateCommand;
import org.apache.oozie.command.jpa.WorkflowJobGetCommand;
import org.apache.oozie.command.jpa.WorkflowJobUpdateCommand;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.WorkflowException;
import org.apache.oozie.workflow.WorkflowInstance;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

public class SuspendXCommand extends WorkflowXCommand<Void> {
    private final String wfid;
    private WorkflowJobBean wfJobBean;
    private JPAService jpaService;

    /**
     * @param id
     */
    public SuspendXCommand(String id) {
        super("suspend", "suspend", 1);
        this.wfid = ParamChecker.notEmpty(id, "wfid");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        incrJobCounter(1);
        try {
            suspendJob(this.jpaService, this.wfJobBean, this.wfid, null);
            jpaService.execute(new WorkflowJobUpdateCommand(this.wfJobBean));
            queue(new NotificationXCommand(this.wfJobBean));
        }
        catch (WorkflowException e) {
            throw new CommandException(e);
        }
        return null;
    }

    /**
     * Suspend the workflow job and pending flag to false for the actions that are START_RETRY or START_MANUAL or
     * END_RETRY or END_MANUAL
     * 
     * @param store WorkflowStore
     * @param workflow WorkflowJobBean
     * @param id String
     * @param actionId String
     * @throws WorkflowException
     * @throws CommandException
     * @throws StoreException
     */
    private void suspendJob(JPAService jpaService, WorkflowJobBean workflow, String id, String actionId)
    throws WorkflowException, CommandException {
        if (workflow.getStatus() == WorkflowJob.Status.RUNNING) {
            workflow.getWorkflowInstance().suspend();
            WorkflowInstance wfInstance = workflow.getWorkflowInstance();
            ((LiteWorkflowInstance) wfInstance).setStatus(WorkflowInstance.Status.SUSPENDED);
            workflow.setStatus(WorkflowJob.Status.SUSPENDED);
            workflow.setWorkflowInstance(wfInstance);

            setPendingFalseForActions(jpaService, id, actionId);
        }
    }

    /**
     * Set pending flag to false for the actions that are START_RETRY or START_MANUAL or END_RETRY or END_MANUAL
     * <p/>
     * 
     * @param store WorkflowStore
     * @param id workflow id
     * @param actionId workflow action id
     * @throws CommandException
     * @throws StoreException
     */
    private void setPendingFalseForActions(JPAService jpaService, String id, String actionId)
    throws CommandException {
        List<WorkflowActionBean> actions = jpaService.execute(new WorkflowActionRetryManualGetCommand(id));
        for (WorkflowActionBean action : actions) {
            if (actionId != null && actionId.equals(action.getId())) {
                // this action has been changed in handleNonTransient()
                continue;
            }
            else {
                action.resetPendingOnly();
            }
            jpaService.execute(new WorkflowActionUpdateCommand(action));
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerLoadState()
     */
    @Override
    protected void eagerLoadState() throws CommandException {
        super.eagerLoadState();
        try {
            jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.wfJobBean = jpaService.execute(new WorkflowJobGetCommand(this.wfid));
            }
            else {
                throw new CommandException(ErrorCode.E0610);
            }
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }
        setLogInfo(this.wfJobBean);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#eagerVerifyPrecondition()
     */
    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        super.eagerVerifyPrecondition();
        if (this.wfJobBean.getStatus() != WorkflowJob.Status.RUNNING) {
            throw new PreconditionException(ErrorCode.E0727, this.wfJobBean.getStatus());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.wfid;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#loadState()
     */
    @Override
    protected void loadState() throws CommandException {

    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
    }
}