package org.apache.oozie.command;

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;
import org.apache.oozie.util.XLog;

public abstract class RerunTransitionXCommand<T> extends TransitionXCommand<T> {
    protected String jobId;

    public RerunTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public RerunTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    @Override
    public final void transitToNext() {
        if (job == null) {
            job = this.getJob();
        }
        job.setStatus(Job.Status.RUNNING);
        job.setPending();
    }

    public abstract void RerunChildren() throws CommandException;

    @Override
    protected T execute() throws CommandException {
        getLog().info("STARTED " + getClass().getSimpleName() + " for jobId=" + jobId);
        transitToNext();
        updateJob();
        RerunChildren();
        notifyParent();
        getLog().info("ENDED " + getClass().getSimpleName() + " for jobId=" + jobId);
        return null;
    }

    @Override
    protected void verifyPrecondition() throws CommandException, PreconditionException {
        eagerVerifyPrecondition();
    }

    @Override
    protected void eagerLoadState() throws CommandException {
        loadState();
    }

    @Override
    protected void eagerVerifyPrecondition() throws CommandException, PreconditionException {
        if (getJob().getStatus() == Job.Status.KILLED || getJob().getStatus() == Job.Status.FAILED) {
            getLog().warn(
                    "RerunCommand is not able to run because job status=" + getJob().getStatus() + ", jobid="
                            + getJob().getId());
            throw new PreconditionException(ErrorCode.E1100, "Not able to rerun the bundle Id= " + getJob().getId()
                    + ". Bundle is in wrong state= " + getJob().getStatus());
        }
    }

    public XLog getLog() {
        return null;
    }
}
