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

import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.action.decision.DecisionActionExecutor;
import org.apache.oozie.client.WorkflowAction;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.service.Services;
import org.apache.oozie.store.StoreException;
import org.apache.oozie.store.WorkflowStore;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;
import org.apache.oozie.workflow.lite.LiteWorkflowApp;
import org.apache.oozie.workflow.lite.LiteWorkflowInstance;

/**
 * Command for loading a job information
 */
public class JobCommand extends WorkflowCommand<WorkflowJobBean> {
    private String id;
    private int start = 1;
    private int len = Integer.MAX_VALUE;

    /**
     * @param id wf jobId
     */
    public JobCommand(String id) {
        this(id, 1, Integer.MAX_VALUE);
    }

    /**
     * @param id wf jobId
     * @param start starting index in the list of actions belonging to the job
     * @param length number of actions to be returned
     */
    public JobCommand(String id, int start, int length) {
        super("job.info", "job.info", 1, XLog.OPS, true);
        this.id = ParamChecker.notEmpty(id, "id");
        this.start = start;
        this.len = length;
    }

    @Override
    protected WorkflowJobBean call(WorkflowStore store) throws StoreException {
        WorkflowJobBean workflow = store.getWorkflowInfoWithActionsSubset(id, start, len);
        workflow.setConsoleUrl(getJobConsoleUrl(id));

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

        return workflow;
    }

    static String getJobConsoleUrl(String jobId) {
        String consoleUrl = Services.get().getConf().get("oozie.JobCommand.job.console.url", null);
        return (consoleUrl != null) ? consoleUrl + jobId : null;
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
        if (executionPathLengthEstimate == 0) { // noop wf
            return 1.0f;
        }
        List<WorkflowAction> actions = wf.getActions();
        int doneActions = 0;
        for (WorkflowAction action : actions) {
            // Skip decision nodes, note start, kill, end, fork/join will not have action entry.
            if (action.getType().equals(DecisionActionExecutor.ACTION_TYPE)) {
                continue;
            }
            if (action.getStatus() == WorkflowAction.Status.OK || action.getStatus() == WorkflowAction.Status.DONE) {
                doneActions++;
            }
        }

        return (doneActions * 1.0f) / executionPathLengthEstimate;
    }
}
