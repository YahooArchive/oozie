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
import org.apache.oozie.service.CallableQueueService;
import org.apache.oozie.service.InstrumentationService;
import org.apache.oozie.service.MemoryLocksService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.MemoryLocks;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for synchronous and asynchronous commands.
 * <p/>
 * It enables by API the following pattern:
 * <p/>
 * <ul>
 * <li>single execution: a command instance can be executed only once</li>
 * <li>eager data loading: loads data for eager precondition check</li>
 * <li>eager precondition check: verify precondition before obtaining lock</li>
 * <li>data loading: loads data for precondition check and execution</li>
 * <li>precondition check: verifies precondition for execution is still met</li>
 * <li>locking: obtains exclusive lock on key before executing the command</li>
 * <li>execution: command logic</li>
 * </ul>
 * <p/>
 * It has built in instrumentation and logging.
 */
public abstract class XCommand<T> implements XCallable<T> {
    public static final String DEFAULT_LOCK_TIMEOUT = "oozie.command.default.lock.timeout";

    private static final String INSTRUMENTATION_GROUP = "commands";

    private static XLog LOG = XLog.getLog(XCommand.class);

    private String name;
    private int priority;
    private String type;
    private long createdTime;
    private MemoryLocks.LockToken lock;
    private boolean used;
    private Map<Long, List<XCommand<T>>> commandQueue;

    /**
     * Create a command.
     * 
     * @param name command name.
     * @param type command type.
     * @param priority command priority.
     */
    public XCommand(String name, String type, int priority) {
        this.name = name;
        this.type = type;
        this.priority = priority;
        createdTime = System.currentTimeMillis();
    }

    /**
     * Return the command name.
     * 
     * @return the command name.
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Return the callable type.
     * <p/>
     * The command type is used for concurrency throttling in the {@link CallableQueueService}.
     * 
     * @return the command type.
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * Return the priority of the command.
     * 
     * @return the command priority.
     */
    @Override
    public int getPriority() {
        return priority;
    }

    /**
     * Returns the creation time of the command.
     * 
     * @return the command creation time, in milliseconds.
     */
    @Override
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * Queue a command for execution after the current command execution completes.
     * <p/>
     * All commands queued during the execution of the current command will be queued for a single serial execution.
     * <p/>
     * If the command execution throws an exception, no command will be effectively queued.
     * 
     * @param command command to queue.
     */
    protected void queue(XCommand<T> command) {
        queue(command, 0);
    }

    /**
     * Queue a command for delayed execution after the current command execution completes.
     * <p/>
     * All commands queued during the execution of the current command with the same delay will be queued for a single
     * serial execution.
     * <p/>
     * If the command execution throws an exception, no command will be effectively queued.
     * 
     * @param command command to queue.
     * @param msDelay delay in milliseconds.
     */
    protected void queue(XCommand<T> command, long msDelay) {
        if (commandQueue == null) {
            commandQueue = new HashMap<Long, List<XCommand<T>>>();
        }
        List<XCommand<T>> list = commandQueue.get(msDelay);
        if (list == null) {
            list = new ArrayList<XCommand<T>>();
            commandQueue.put(msDelay, list);
        }
        list.add(command);
    }

    /**
     * Obtain an exclusive lock on the {link #getEntityKey}.
     * <p/>
     * A timeout of {link #getLockTimeOut} is used when trying to obtain the lock.
     * 
     * @throws InterruptedException thrown if an interruption happened while trying to obtain the lock
     * @throws CommandException thrown i the lock could not be obtained.
     */
    private void acquireLock() throws InterruptedException, CommandException {
        lock = Services.get().get(MemoryLocksService.class).getWriteLock(getEntityKey(), getLockTimeOut());
        if (lock == null) {
            Instrumentation instrumentation = Services.get().get(InstrumentationService.class).get();
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".lockTimeOut", 1);
            throw new CommandException(ErrorCode.E0606, this.toString(), getLockTimeOut());
        }
        getLog().debug("Acquired lock for [{0}]", getEntityKey());
    }

    /**
     * Release the lock on the {link #getEntityKey}.
     */
    private void releaseLock() {
        if (lock != null) {
            lock.release();
            getLog().debug("Released lock for [{0}]", getEntityKey());
        }
    }

    /**
     * Implements the XCommand life-cycle.
     * 
     * @return the {link #execute} return value.
     * @throws Exception thrown if the command could not be executed.
     */
    @Override
    public final T call() throws Exception {
        if (used) {
            throw new IllegalStateException("XCommand already used");
        }
        used = true;
        Instrumentation instrumentation = Services.get().get(InstrumentationService.class).get();
        instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".executions", 1);
        Instrumentation.Cron callCron = new Instrumentation.Cron();
        try {
            callCron.start();
            eagerLoadState();
            eagerVerifyPrecondition();
            try {
                if (isLockRequired()) {
                    Instrumentation.Cron acquireLockCron = new Instrumentation.Cron();
                    try {
                        acquireLockCron.start();
                        acquireLock();
                        acquireLockCron.stop();
                    }
                    finally {
                        instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".acquireLock", acquireLockCron);
                    }
                }
                getLog().debug("Load state for [{0}]", getEntityKey());
                loadState();
                getLog().debug("Precondition check for command [{0}] key [{1}]", getName(), getEntityKey());
                verifyPrecondition();
                getLog().debug("Execute command [{0}] key [{1}]", getName(), getEntityKey());
                Instrumentation.Cron executeCron = new Instrumentation.Cron();
                T ret;
                try {
                    executeCron.start();
                    ret = execute();
                }
                finally {
                    executeCron.stop();
                    instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".execute", executeCron);
                }
                if (commandQueue != null) {
                    CallableQueueService callableQueueService = Services.get().get(CallableQueueService.class);
                    for (Map.Entry<Long, List<XCommand<T>>> entry : commandQueue.entrySet()) {
                        getLog().debug("Queuing [{0}] commands with delay [{1}]ms", entry.getValue().size(),
                                entry.getKey());
                        if (!callableQueueService.queueSerial(entry.getValue(), entry.getKey())) {
                            getLog().warn("Could not queue [{0}] commands with delay [{1}]ms, queue full",
                                    entry.getValue().size(), entry.getKey());
                        }
                    }
                }
                return ret;
            }
            finally {
                if (isLockRequired()) {
                    releaseLock();
                }
            }
        }
        catch (Exception ex) {
            instrumentation.incr(INSTRUMENTATION_GROUP, getName() + ".exceptions", 1);
            throw ex;
        }
        finally {
            callCron.stop();
            instrumentation.addCron(INSTRUMENTATION_GROUP, getName() + ".call", callCron);
        }
    }

    /**
     * Return the {link XLog} instance used by the command.
     * <p/>
     * The log instance belongs to the {link XCommand}.
     * <p/>
     * Subclasses should override this method if the want to use a different log instance.
     * 
     * @return the log instance.
     */
    protected XLog getLog() {
        return LOG;
    }

    /**
     * Return the time out when acquiring a lock.
     * <p/>
     * The value is loaded from the Oozie configuration, the property {link #DEFAULT_LOCK_TIMEOUT}.
     * <p/>
     * Subclasses should override this method if they want to use a different time out.
     * 
     * @return the lock time out in milliseconds.
     */
    protected long getLockTimeOut() {
        return Services.get().getConf().getLong(DEFAULT_LOCK_TIMEOUT, 5 * 1000);
    }

    /**
     * Indicate if the the command requires locking.
     * <p/>
     * Subclasses should override this method if they require locking.
     * 
     * @return <code>false</code>
     */
    protected boolean isLockRequired() {
        return false;
    }

    /**
     * Return the entity key for the command.
     * <p/>
     * 
     * @return the entity key for the command.
     */
    protected abstract String getEntityKey();

    /**
     * Load the necessary state to perform an eager precondition check.
     * <p/>
     * This implementation does a NOP.
     * <p/>
     * Subclasses should override this method and load the state needed to do an eager precondition check.
     * <p/>
     * A trivial implementation is calling {link #loadState}.
     */
    protected void eagerLoadState() {
    }

    /**
     * Verify the precondition for the command before obtaining a lock.
     * <p/>
     * This implementation does a NOP.
     * <p/>
     * A trivial implementation is calling {link #verifyPrecondition}.
     * 
     * @throws CommandException thrown if the precondition is not met.
     */
    protected void eagerVerifyPrecondition() throws CommandException {
    }

    /**
     * Load the necessary state to perform the precondition check and to execute the command.
     * <p/>
     * Subclasses must implement this method and load the state needed to do the precondition check and execute the
     * command.
     */
    protected abstract void loadState();

    /**
     * Verify the precondition for the command after a lock has been obtain, just before executing the command.
     * <p/>
     * 
     * @throws CommandException thrown if the precondition is not met.
     */
    protected abstract void verifyPrecondition() throws CommandException;

    /**
     * Command execution body.
     * <p/>
     * This method will be invoked after the {link #loadState} and {link #verifyPrecondition} methods.
     * <p/>
     * If the command requires locking, this method will be invoked ONLY if the lock has been acquired.
     * 
     * @return a return value from the execution of the command, only meaningful if the command is executed
     *         synchronously.
     * @throws CommandException thrown if the command execution failed.
     */
    protected abstract T execute() throws CommandException;

}
