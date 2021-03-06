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
package org.apache.oozie.service;

import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.util.XLog;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This service executes scheduled Runnables and Callables at regular intervals. <p/> It uses a
 * java.util.concurrent.ScheduledExecutorService. <p/> The {@link #SCHEDULER_THREADS} configuration property indicates
 * how many threads the scheduler will use to run scheduled commands.
 */
public class SchedulerService implements Service {

    public static final String CONF_PREFIX = Service.CONF_PREFIX + "SchedulerService.";

    public static final String SCHEDULER_THREADS = CONF_PREFIX + "threads";

    private final XLog log = XLog.getLog(getClass());

    private ScheduledExecutorService scheduler;

    /**
     * Initialize the scheduler service.
     *
     * @param services services instance.
     */
    @Override
    public void init(Services services) {
        scheduler = new ScheduledThreadPoolExecutor(services.getConf().getInt(SCHEDULER_THREADS, 5));
    }

    /**
     * Destroy the scheduler service.
     */
    @Override
    public void destroy() {
        try {
            long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
            scheduler.shutdownNow();
            while (!scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                log.info("Waiting for scheduler to shutdown");
                if (System.currentTimeMillis() > limit) {
                    log.warn("Gave up, continuing without waiting for scheduler to shutdown");
                    break;
                }
            }
        }
        catch (InterruptedException ex) {
            log.warn(ex);
        }
    }

    /**
     * Return the public interface for scheduler service.
     *
     * @return {@link SchedulerService}.
     */
    @Override
    public Class<? extends Service> getInterface() {
        return SchedulerService.class;
    }

    /**
     * Return the java.util.concurrent.ScheduledExecutorService instance used by the SchedulerService. <p/>
     *
     * @return the scheduled executor service instance.
     */
    public ScheduledExecutorService getScheduler() {
        return scheduler;
    }

    public enum Unit {
        MILLISEC(1),
        SEC(1000),
        MIN(1000 * 60),
        HOUR(1000 * 60 * 60);

        private long millis;

        private Unit(long millis) {
            this.millis = millis;
        }

        private long getMillis() {
            return millis;
        }

    }

    /**
     * Schedule a Callable for execution.
     *
     * @param callable callable to schedule for execution.
     * @param delay delay for first execution since scheduling.
     * @param interval interval between executions.
     * @param unit scheduling unit.
     */
    public void schedule(final Callable<Void> callable, long delay, long interval, Unit unit) {
        log.trace("Scheduling callable [{0}], interval [{1}] seconds, delay [{2}] in [{3}]",
                  callable.getClass(), delay, interval, unit);
        Runnable r = new Runnable() {
            public void run() {
                if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
                    log.trace("schedule[run/callable] System is in SAFEMODE. Therefore nothing will run");
                    return;
                }
                try {
                    callable.call();
                }
                catch (Exception ex) {
                    log.warn("Error executing callable [{0}], {1}", callable.getClass().getSimpleName(),
                             ex.getMessage(), ex);
                }
            }
        };
        if (!scheduler.isShutdown()) {
            scheduler.scheduleWithFixedDelay(r, delay * unit.getMillis(), interval * unit.getMillis(),
                                             TimeUnit.MILLISECONDS);
        }
        else {
            log.warn("Scheduler shutting down, ignoring scheduling of [{0}]", callable.getClass());
        }
    }

    /**
     * Schedule a Runnable for execution.
     *
     * @param runnable Runnable to schedule for execution.
     * @param delay delay for first execution since scheduling.
     * @param interval interval between executions.
     * @param unit scheduling unit.
     */
    public void schedule(final Runnable runnable, long delay, long interval, Unit unit) {
        log.trace("Scheduling runnable [{0}], interval [{1}], delay [{2}] in [{3}]",
                  runnable.getClass(), delay, interval, unit);
        Runnable r = new Runnable() {
            public void run() {
                if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
                    log.trace("schedule[run/Runnable] System is in SAFEMODE. Therefore nothing will run");
                    return;
                }
                try {
                    runnable.run();
                }
                catch (Exception ex) {
                    log.warn("Error executing runnable [{0}], {1}", runnable.getClass().getSimpleName(),
                             ex.getMessage(), ex);
                }
            }
        };
        if (!scheduler.isShutdown()) {
            scheduler.scheduleWithFixedDelay(r, delay * unit.getMillis(), interval * unit.getMillis(),
                                                 TimeUnit.MILLISECONDS);
        }
        else {
            log.warn("Scheduler shutting down, ignoring scheduling of [{0}]", runnable.getClass());
        }
    }

}
