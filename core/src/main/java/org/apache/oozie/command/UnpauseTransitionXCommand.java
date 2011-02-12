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

import org.apache.oozie.ErrorCode;
import org.apache.oozie.client.Job;

public abstract class UnpauseTransitionXCommand extends TransitionXCommand<Void> {

    public UnpauseTransitionXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    public abstract void unpauseChildren() throws CommandException;

    @Override
    public final void transitToNext() throws CommandException {
        if (job == null) {
            job = this.getJob();
        }
        
        if (job.getStatus() == Job.Status.PAUSED) {
            job.setStatus(Job.Status.RUNNING);
        }
        else if (job.getStatus() == Job.Status.PAUSEDWITHERROR) {
            job.setStatus(Job.Status.RUNNINGWITHERROR);
        }
        else if (job.getStatus() == Job.Status.PREPPAUSED) {
            job.setStatus(Job.Status.PREP);
        }
        else {
            throw new CommandException(ErrorCode.E1316, job.getId());
        }

        //TODO: to be revisited;
        //job.setPending();
    }

    @Override
    protected Void execute() throws CommandException {
        loadState();
        transitToNext();
        updateJob();
        unpauseChildren();
        notifyParent();
        return null;
    }
}
