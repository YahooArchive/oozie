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

import org.apache.oozie.util.XLog;

/**
 * Transition command for update status.
 */
public abstract class StatusUpdateXCommand extends TransitionXCommand<Void>{

    protected final XLog LOG = XLog.getLog(StatusUpdateXCommand.class);

    /**
     * The constructor for abstract class {@link StatusUpdateXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     */
    public StatusUpdateXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * The constructor for abstract class {@link StatusUpdateXCommand}
     *
     * @param name the command name
     * @param type the command type
     * @param priority the command priority
     * @param dryrun true if dryrun is enable
     */
    public StatusUpdateXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }
}
