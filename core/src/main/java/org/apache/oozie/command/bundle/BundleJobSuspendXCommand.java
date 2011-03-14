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
package org.apache.oozie.command.bundle;

import java.util.Date;
import java.util.List;

import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.ErrorCode;
import org.apache.oozie.XException;
import org.apache.oozie.client.Job;
import org.apache.oozie.command.CommandException;
import org.apache.oozie.command.PreconditionException;
import org.apache.oozie.command.SuspendTransitionXCommand;
import org.apache.oozie.command.coord.CoordSuspendXCommand;
import org.apache.oozie.executor.jpa.BundleActionUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.BundleActionsGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobGetJPAExecutor;
import org.apache.oozie.executor.jpa.BundleJobUpdateJPAExecutor;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.InstrumentUtils;
import org.apache.oozie.util.ParamChecker;
import org.apache.oozie.util.XLog;

public class BundleJobSuspendXCommand extends SuspendTransitionXCommand {
    private final String jobId;
    private JPAService jpaService;
    private List<BundleActionBean> bundleActions;
    private BundleJobBean bundleJob;
    private final XLog LOG = XLog.getLog(BundleJobSuspendXCommand.class);

    public BundleJobSuspendXCommand(String id) {
        super("bundle_suspend", "bundle_suspend", 1);
        this.jobId = ParamChecker.notEmpty(id, "id");
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#getJob()
     */
    @Override
    public Job getJob() {
        return bundleJob;
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#notifyParent()
     */
    @Override
    public void notifyParent() throws CommandException {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#setJob(org.apache.oozie.client.Job)
     */
    @Override
    public void setJob(Job job) {
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#getEntityKey()
     */
    @Override
    protected String getEntityKey() {
        return this.jobId;
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
        jpaService = Services.get().get(JPAService.class);
        if (jpaService == null) {
            throw new CommandException(ErrorCode.E0610);
        }

        try {
            bundleJob = jpaService.execute(new BundleJobGetJPAExecutor(jobId));
        }
        catch (Exception Ex) {
            throw new CommandException(ErrorCode.E0604, jobId);
        }

        try {
            bundleActions = jpaService.execute(new BundleActionsGetJPAExecutor(jobId));
        }
        catch (Exception Ex) {
            throw new CommandException(ErrorCode.E1311, jobId);
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#verifyPrecondition()
     */
    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        if (bundleJob.getStatus() == Job.Status.SUCCEEDED || bundleJob.getStatus() == Job.Status.FAILED
                || bundleJob.getStatus() == Job.Status.KILLED) {
            LOG.info("BundleJobSuspendXCommand is not going to execute because job finished or failed or killed, id = "
                            + jobId + ", status = " + bundleJob.getStatus());
            throw new PreconditionException(ErrorCode.E1312, jobId, bundleJob.getStatus().toString());
        }
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#updateJob()
     */
    @Override
    public void updateJob() throws CommandException {
        InstrumentUtils.incrJobCounter("bundle_suspend", 1, null);
        bundleJob.setPending();
        bundleJob.setSuspendedTime(new Date());
        bundleJob.setLastModifiedTime(new Date());

        LOG.debug("Suspend bundle job id = " + jobId + ", status = " + bundleJob.getStatus() + ", pending = " + bundleJob.isPending());
        try {
            jpaService.execute(new BundleJobUpdateJPAExecutor(bundleJob));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }

    @Override
    public void suspendChildren() throws CommandException {
        try {
            for (BundleActionBean action : this.bundleActions) {
                if (action.getStatus() == Job.Status.RUNNING || action.getStatus() == Job.Status.PREP) {
                    // queue a CoordSuspendXCommand
                    if (action.getCoordId() != null) {
                        queue(new CoordSuspendXCommand(action.getCoordId()));
                        updateBundleAction(action);
                        LOG.debug("Suspend bundle action = [{0}], new status = [{1}], pending = [{2}] and queue CoordSuspendXCommand for [{3}]",
                                action.getBundleActionId(), action.getStatus(), action.getPending(), action.getCoordId());
                    } else {
                        updateBundleAction(action);
                        LOG.debug("Suspend bundle action = [{0}], new status = [{1}], pending = [{2}] and coord id is null",
                                action.getBundleActionId(), action.getStatus(), action.getPending());
                    }

                }
            }
            LOG.debug("Suspended bundle actions for the bundle=[{0}]", jobId);
        }
        catch (XException ex) {
            throw new CommandException(ex);
        }
    }

    private void updateBundleAction(BundleActionBean action) throws CommandException {
        if (action.getStatus() == Job.Status.PREP) {
            action.setStatus(Job.Status.PREPSUSPENDED);
        }
        else if (action.getStatus() == Job.Status.RUNNING) {
            action.setStatus(Job.Status.SUSPENDED);
        }
        action.incrementAndGetPending();
        action.setLastModifiedTime(new Date());
        try {
            jpaService.execute(new BundleActionUpdateJPAExecutor(action));
        }
        catch (JPAExecutorException e) {
            throw new CommandException(e);
        }
    }
}
