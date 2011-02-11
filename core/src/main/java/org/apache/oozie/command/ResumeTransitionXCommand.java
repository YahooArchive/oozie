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
package org.apache.oozie.command;

import org.apache.oozie.client.Job;

public abstract class ResumeTransitionXCommand extends TransitionXCommand<Void>{

    /**
     * Submit the job
     *
     * @return String
     * @throws CommandException
     */
    public abstract void resume() throws CommandException;

    /**
     * @param name
     * @param type
     * @param priority
     */
    public ResumeTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * @param name
     * @param type
     * @param priority
     * @param dryrun
     */
    public ResumeTransitionXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.TransitionXCommand#transitToNext()
     */
    @Override
    public void transitToNext() {
        if (job == null) {
            job = this.getJob();
        }
        if(job.getStatus() == Job.Status.PREPSUSPENDED){
            job.setStatus(Job.Status.PREP);
        }else if(job.getStatus() == Job.Status.SUSPENDED){
            job.setStatus(Job.Status.RUNNING);
        }
        job.resetPending();
    }

    /* (non-Javadoc)
     * @see org.apache.oozie.command.XCommand#execute()
     */
    @Override
    protected Void execute() throws CommandException {
        transitToNext();
        resume();
        notifyParent();
        return null;
    }
}
