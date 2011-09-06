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
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.decision.DecisionActionExecutor;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.executor.jpa.WorkflowInfoWithActionsSubsetGetJPAExecutor;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;

/**
 * This Xcommand is returning the workflow with action within the range.
 */
public class JobXCommand extends WorkflowXCommand<WorkflowJobBean> {
    private final String id;
    private final int start = 1;
    private final int len = Integer.MAX_VALUE;
    private WorkflowJobBean workflow;

    public JobXCommand(String id) {
        this(id, 1, Integer.MAX_VALUE);
    }

    /**
     * Constructor used to retrieve WF Job
     * @param id wf jobId
     * @param start starting index in the list of actions belonging to the job
     * @param length number of actions to be returned
     */
    public JobXCommand(String id, int start, int length) {
        super("job.info", "job.info", 1, true);
        this.id = ParamChecker.notEmpty(id, "id");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected WorkflowJobBean execute() throws CommandException {
        try {
            JPAService jpaService = Services.get().get(JPAService.class);
            if (jpaService != null) {
                this.workflow = jpaService.execute(new WorkflowInfoWithActionsSubsetGetJPAExecutor(this.id, this.start,
                        this.len));
            }
            else {
                throw new CommandException(ErrorCode.E0610, this.id);
            }
            this.workflow.setConsoleUrl(getJobConsoleUrl(id));
        
            // Estimate job progress
            if (workflow.getStatus() == WorkflowJob.Status.PREP) {
                workflow.setProgress(0.0f);
            }
            else if (workflow.getStatus() == WorkflowJob.Status.SUCCEEDED) {
                workflow.setProgress(1.0f);
            }
            else {
                workflow.setProgress(getJobProgress(workflow));
            }
        }
        catch (JPAExecutorException ex) {
            throw new CommandException(ex);
        }
        catch (Exception ex) {
            throw new CommandException(ErrorCode.E0603, ex);
        }

        return this.workflow;
    }

    /**
     * @param jobId : Job ID to retrieve console URL
     * @return console URL
     */
    static String getJobConsoleUrl(String jobId) {
        String consoleUrl = Services.get().getConf().get("oozie.JobCommand.job.console.url", null);
        return (consoleUrl != null) ? consoleUrl + jobId : null;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.id;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#isLockRequired()
     */
    @Override
    protected boolean isLockRequired() {
        return false;
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

    /**
     * Compute job progress that is defined as fraction of done actions. 
     * 
     * @param wf workflow job bean
     * @return job progress
     */
    static float getJobProgress(WorkflowJobBean wf) {
        LiteWorkflowInstance wfInstance = (LiteWorkflowInstance) wf.getWorkflowInstance();
        LiteWorkflowApp wfApp = (LiteWorkflowApp) wfInstance.getApp();

        int executionPathLengthEstimate = wfApp.getExecutionPathLengthEstimate();
        float progress;
        if (executionPathLengthEstimate == 0) { // noop wf
            progress = 1.0f;
        }
        else {
            List<WorkflowAction> actions = wf.getActions();
            int doneActions = 0;
            for (WorkflowAction action : actions) {
                // Skip decision nodes, note start, kill, end, fork/join will not have action entry.
                if (action.getType().equals(DecisionActionExecutor.ACTION_TYPE)) {
                    // noop
                }
                else {
                    // Make progress if an action is in terminal state
                    if (action.getStatus() == WorkflowAction.Status.OK
                            || action.getStatus() == WorkflowAction.Status.ERROR
                            || action.getStatus() == WorkflowAction.Status.FAILED
                            || action.getStatus() == WorkflowAction.Status.KILLED) {
                        doneActions++;
                    }
                }
            }

            progress = (doneActions * 1.0f) / executionPathLengthEstimate;
        }

        return progress;
    }
}
