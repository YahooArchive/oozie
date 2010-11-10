package org.apache.oozie.command.wf;

import org.apache.oozie.command.XCommand;

/**
 * Abstract coordinator command class derived from XCommand
 *
 * @param <T>
 */
public abstract class WorkflowXCommand<T> extends XCommand<T> {
    /**
     * @param name
     * @param type
     * @param priority
     */
    public WorkflowXCommand(String name, String type, int priority) {
        super(name, type, priority);
    }

    /**
     * @param name
     * @param type
     * @param priority
     * @param dryrun
     */
    public WorkflowXCommand(String name, String type, int priority, boolean dryrun) {
        super(name, type, priority, dryrun);
    }

}