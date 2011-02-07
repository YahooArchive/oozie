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

import org.apache.oozie.command.XCommand;

/**
 * Abstract coordinator command class derived from XCommand
 *
 * @param <T>
 */
public abstract class WorkflowXCommand<T> extends XCommand<T> {
    /**
     * Base class constructor for workflow commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     */
    public WorkflowXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * Base class constructor for workflow commands.
     *
     * @param name command name
     * @param type command type
     * @param priority command priority
     * @param dryrun true if rerun is enabled for command
     */
    public WorkflowXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

}