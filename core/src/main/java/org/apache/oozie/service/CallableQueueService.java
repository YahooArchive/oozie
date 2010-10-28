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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.oozie.client.OozieClient.SYSTEM_MODE;
import org.apache.oozie.util.Instrumentable;
import org.apache.oozie.util.Instrumentation;
import org.apache.oozie.util.PriorityDelayQueue;
import org.apache.oozie.util.XCallable;
import org.apache.oozie.util.XLog;
import org.apache.oozie.util.PriorityDelayQueue.QueueElement;

/**
 * The callable queue service queues {@link XCallable}s for asynchronous
 * execution.
 * <p/>
 * Callables can be queued for immediate execution or for delayed execution
 * (some time in the future).
 * <p/>
 * Callables are consumed from the queue for execution based on their priority.
 * <p/>
 * When the queues (for immediate execution and for delayed execution) are full,
 * the callable queue service stops queuing callables.
 * <p/>
 * A thread-pool is used to execute the callables asynchronously.
 * <p/>
 * The following configuration parameters control the callable queue service:
 * <p/>
 * {@link #CONF_QUEUE_SIZE} size of the immediate execution queue. Defaulf value
 * is 10000.
 * <p/>
 * {@link #CONF_THREADS} number of threads in the thread-pool used for
 * asynchronous command execution. When this number of threads is reached,
 * commands remain the queue until threads become available. Sets up a priority
 * queue for the execution of Commands via a ThreadPool. Sets up a Delayed Queue
 * to handle actions which will be ready for execution sometime in the future.
 */
public class CallableQueueService implements Service, Instrumentable {
	private static final String INSTRUMENTATION_GROUP = "callablequeue";
	private static final String INSTR_IN_QUEUE_TIME_TIMER = "time.in.queue";
	private static final String INSTR_EXECUTED_COUNTER = "executed";
	private static final String INSTR_FAILED_COUNTER = "failed";
	private static final String INSTR_QUEUED_COUNTER = "queued";
	private static final String INSTR_QUEUE_SIZE_SAMPLER = "queue.size";
	private static final String INSTR_THREADS_ACTIVE_SAMPLER = "threads.active";

	public static final String CONF_PREFIX = Service.CONF_PREFIX
			+ "CallableQueueService.";

	public static final String CONF_QUEUE_SIZE = CONF_PREFIX + "queue.size";
	public static final String CONF_THREADS = CONF_PREFIX + "threads";
	public static final String CONF_CALLABLE_CONCURRENCY = CONF_PREFIX
			+ "callable.concurrency";

	public static final int CONCURRENCY_DELAY = 500;

	public static final int SAFE_MODE_DELAY = 60000;

	final private Map<String, AtomicInteger> activeCallables = new HashMap<String, AtomicInteger>();
	private int maxCallableConcurrency;

	private boolean callableBegin(XCallable callable) {
		synchronized (activeCallables) {
			AtomicInteger counter = activeCallables.get(callable.getType());
			if (counter == null) {
				counter = new AtomicInteger(1);
				activeCallables.put(callable.getType(), counter);
				return true;
			} else {
				int i = counter.incrementAndGet();
				return i <= maxCallableConcurrency;
			}
		}
	}

	private void callableEnd(XCallable callable) {
		synchronized (activeCallables) {
			AtomicInteger counter = activeCallables.get(callable.getType());
			if (counter == null) {
				throw new IllegalStateException("It should not happen");
			} else {
				int i = counter.decrementAndGet();
			}
		}
	}

	// Callables are wrapped with the this wrapper for execution, for logging
	// and instrumentation.
	// The wrapper implements Runnable and Comparable to be able to work with an
	// executor and a priority queue.
	class CallableWrapper extends PriorityDelayQueue.QueueElement<XCallable<?>>
			implements Runnable {
		private Instrumentation.Cron cron;

		public CallableWrapper(XCallable<?> callable, long delay) {
			super(callable, callable.getPriority(), delay,
					TimeUnit.MILLISECONDS);
			cron = new Instrumentation.Cron();
			cron.start();
		}

		public void run() {
			if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
				log
						.info(
								"Oozie is in SAFEMODE, requeuing callable [{0}] with [{1}]ms delay",
								getElement().getType(), SAFE_MODE_DELAY);
				setDelay(SAFE_MODE_DELAY, TimeUnit.MILLISECONDS);
				queue(this, true);
				return;
			}
			XCallable<?> callable = getElement();
			try {
				if (callableBegin(callable)) {
					cron.stop();
					addInQueueCron(cron);
					XLog.Info.get().clear();
					XLog log = XLog.getLog(getClass());
					log.trace("executing callable [{0}]", callable.getName());
					try {
						callable.call();
						incrCounter(INSTR_EXECUTED_COUNTER, 1);
						log
								.trace("executed callable [{0}]", callable
										.getName());
					} catch (Exception ex) {
						incrCounter(INSTR_FAILED_COUNTER, 1);
						log.warn("exception callable [{0}], {1}", callable
								.getName(), ex.getMessage(), ex);
					} finally {
						XLog.Info.get().clear();
					}
				} else {
					log
							.warn(
									"max concurrency for callable [{0}] exceeded, requeueing with [{1}]ms delay",
									callable.getType(), CONCURRENCY_DELAY);
					setDelay(CONCURRENCY_DELAY, TimeUnit.MILLISECONDS);
					queue(this, true);
					incrCounter(callable.getType() + "#exceeded.concurrency", 1);
				}
			} finally {
				callableEnd(callable);
			}
		}

		/**
		 * @return String the queue dump
		 */
		@Override
		public String toString() {
			return "delay=" + getDelay(TimeUnit.MILLISECONDS) + ", elements="
					+ getElement().toString();
		}

	}

	class CompositeCallable implements XCallable<Void> {
		private List<XCallable<?>> callables;
		private String name;
		private int priority;
		private long createdTime;

		public CompositeCallable(List<? extends XCallable<?>> callables) {
			this.callables = new ArrayList<XCallable<?>>(callables);
			priority = 0;
			createdTime = Long.MAX_VALUE;
			StringBuilder sb = new StringBuilder();
			String separator = "[";
			for (XCallable<?> callable : callables) {
				priority = Math.max(priority, callable.getPriority());
				createdTime = Math.min(createdTime, callable.getCreatedTime());
				sb.append(separator).append(callable.getName());
				separator = ",";
			}
			sb.append("]");
			name = sb.toString();
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getType() {
			return "#composite#" + callables.get(0).getType();
		}

		@Override
		public int getPriority() {
			return priority;
		}

		@Override
		public long getCreatedTime() {
			return createdTime;
		}

		public Void call() throws Exception {
			XLog log = XLog.getLog(getClass());

			for (XCallable<?> callable : callables) {
				log.trace("executing callable [{0}]", callable.getName());
				try {
					callable.call();
					incrCounter(INSTR_EXECUTED_COUNTER, 1);
					log.trace("executed callable [{0}]", callable.getName());
				} catch (Exception ex) {
					incrCounter(INSTR_FAILED_COUNTER, 1);
					log.warn("exception callable [{0}], {1}", callable
							.getName(), ex.getMessage(), ex);
				}
			}

			// ticking -1 not to count the call to the composite callable
			incrCounter(INSTR_EXECUTED_COUNTER, -1);
			return null;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			if (callables.size() == 0) {
				return null;
			}
			StringBuilder sb = new StringBuilder();
			int size = callables.size();
			for (int i = 0; i < size; i++) {
				XCallable<?> callable = callables.get(i);
				sb.append("(");
				sb.append(callable.toString());
				if (i + 1 == size) {
					sb.append(")");
				} else {
					sb.append("),");
				}
			}
			return sb.toString();
		}

	}

	private XLog log = XLog.getLog(getClass());

	private int queueSize;
	private PriorityDelayQueue<CallableWrapper> queue;
	private AtomicLong delayQueueExecCounter = new AtomicLong(0);
	private ThreadPoolExecutor executor;
	private Instrumentation instrumentation;

	/**
	 * Convenience method for instrumentation counters.
	 * 
	 * @param name
	 *            counter name.
	 * @param count
	 *            count to increment the counter.
	 */
	private void incrCounter(String name, int count) {
		if (instrumentation != null) {
			instrumentation.incr(INSTRUMENTATION_GROUP, name, count);
		}
	}

	private void addInQueueCron(Instrumentation.Cron cron) {
		if (instrumentation != null) {
			instrumentation.addCron(INSTRUMENTATION_GROUP,
					INSTR_IN_QUEUE_TIME_TIMER, cron);
		}
	}

	/**
	 * Initialize the command queue service.
	 * 
	 * @param services
	 *            services instance.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public void init(Services services) {
		Configuration conf = services.getConf();

		queueSize = conf.getInt(CONF_QUEUE_SIZE, 10000);
		int threads = conf.getInt(CONF_THREADS, 10);

		queue = new PriorityDelayQueue<CallableWrapper>(3, 1000 * 30,
				TimeUnit.MILLISECONDS, queueSize) {
			@Override
			protected void debug(String msgTemplate, Object... msgArgs) {
				log.trace(msgTemplate, msgArgs);
			}
		};

		// IMPORTANT: The ThreadPoolExecutor does not always the execute
		// commands out of the queue, there are
		// certain conditions where commands are pushed directly to a thread.
		// As we are using a queue with DELAYED semantics (i.e. execute the
		// command in 5 mins) we need to make
		// sure that the commands are always pushed to the queue.
		// To achieve this (by looking a the ThreadPoolExecutor.execute()
		// implementation, we are making the pool
		// minimum size equals to the maximum size (thus threads are keep always
		// running) and we are warming up
		// all those threads (the for loop that runs dummy runnables).
		executor = new ThreadPoolExecutor(threads, threads, 10,
				TimeUnit.SECONDS, (BlockingQueue) queue);

		for (int i = 0; i < threads; i++) {
			executor.execute(new Runnable() {
				public void run() {
					try {
						Thread.sleep(100);
					} catch (InterruptedException ex) {
						log.warn("Could not warm up threadpool {0}", ex
								.getMessage(), ex);
					}
				}
			});
		}

		maxCallableConcurrency = conf.getInt(CONF_CALLABLE_CONCURRENCY, 3);
	}

	/**
	 * Destroy the command queue service.
	 */
	@Override
	public void destroy() {
		try {
			long limit = System.currentTimeMillis() + 30 * 1000;// 30 seconds
			executor.shutdown();
			queue.clear();
			while (!executor.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
				log.info("Waiting for executor to shutdown");
				if (System.currentTimeMillis() > limit) {
					log
							.warn("Gave up, continuing without waiting for executor to shutdown");
					break;
				}
			}
		} catch (InterruptedException ex) {
			log.warn(ex);
		}
	}

	/**
	 * Return the public interface for command queue service.
	 * 
	 * @return {@link CallableQueueService}.
	 */
	@Override
	public Class<? extends Service> getInterface() {
		return CallableQueueService.class;
	}

	/**
	 * @return int size of queue
	 */
	public synchronized int queueSize() {
		return queue.size();
	}

	private boolean queue(CallableWrapper wrapper, boolean ignoreQueueSize) {
		if (!ignoreQueueSize && queue.size() >= queueSize) {
			log.warn("queue if full, ignoring queuing for [{0}]", wrapper
					.getElement());
			return false;
		}
		if (!executor.isShutdown()) {
			executor.execute(wrapper);
		} else {
			log.warn("Executor shutting down, ignoring queueing of [{0}]",
					wrapper.getElement());
		}
		return true;
	}

	/**
	 * Queue a callable for asynchronous execution.
	 * 
	 * @param callable
	 *            callable to queue.
	 * @return <code>true</code> if the callable was queued, <code>false</code>
	 *         if the queue is full and the callable was not queued.
	 */
	public boolean queue(XCallable<?> callable) {
		return queue(callable, 0);
	}

	/**
	 * Queue a list of callables for serial execution.
	 * <p/>
	 * Useful to serialize callables that may compete with each other for
	 * resources.
	 * <p/>
	 * All callables will be processed with the priority of the highest priority
	 * of all callables.
	 * 
	 * @param callables
	 *            callables to be executed by the composite callable.
	 * @return <code>true</code> if the callables were queued,
	 *         <code>false</code> if the queue is full and the callables were
	 *         not queued.
	 */
	@SuppressWarnings("unchecked")
	public boolean queueSerial(List<? extends XCallable<?>> callables) {
		return queueSerial(callables, 0);
	}

	/**
	 * Queue a callable for asynchronous execution sometime in the future.
	 * 
	 * @param callable
	 *            callable to queue for delayed execution
	 * @param delay
	 *            time, in milliseconds, that the callable should be delayed.
	 * @return <code>true</code> if the callable was queued, <code>false</code>
	 *         if the queue is full and the callable was not queued.
	 */
	public synchronized boolean queue(XCallable<?> callable, long delay) {
		if (callable == null) {
			return true;
		}
		boolean queued = false;
		if (Services.get().getSystemMode() == SYSTEM_MODE.SAFEMODE) {
			log
					.warn("[queue] System is in SAFEMODE. Hence no callable is queued. current queue size "
							+ queue.size());
		} else {
			queued = queue(new CallableWrapper(callable, delay), false);
			if (queued) {
				incrCounter(INSTR_QUEUED_COUNTER, 1);
			} else {
				log.warn("Could not queue callable");
			}
		}
		return queued;
	}

	/**
	 * Queue a list of callables for serial execution sometime in the future.
	 * <p/>
	 * Useful to serialize callables that may compete with each other for
	 * resources.
	 * <p/>
	 * All callables will be processed with the priority of the highest priority
	 * of all callables.
	 * 
	 * @param callables
	 *            callables to be executed by the composite callable.
	 * @param delay
	 *            time, in milliseconds, that the callable should be delayed.
	 * @return <code>true</code> if the callables were queued,
	 *         <code>false</code> if the queue is full and the callables were
	 *         not queued.
	 */
	@SuppressWarnings("unchecked")
	public synchronized boolean queueSerial(
			List<? extends XCallable<?>> callables, long delay) {
		boolean queued;
		if (callables == null || callables.size() == 0) {
			queued = true;
		} else if (callables.size() == 1) {
			queued = queue(callables.get(0), delay);
		} else {
			XCallable<?> callable = new CompositeCallable(callables);
			queued = queue(callable, delay);
			if (queued) {
				incrCounter(INSTR_QUEUED_COUNTER, callables.size());
			}
		}
		return queued;
	}

	/**
	 * Instruments the callable queue service.
	 * 
	 * @param instr
	 *            instance to instrument the callable queue service to.
	 */
	public void instrument(Instrumentation instr) {
		instrumentation = instr;
		instr.addSampler(INSTRUMENTATION_GROUP, INSTR_QUEUE_SIZE_SAMPLER, 60,
				1, new Instrumentation.Variable<Long>() {
					public Long getValue() {
						return (long) queue.size();
					}
				});
		instr.addSampler(INSTRUMENTATION_GROUP, INSTR_THREADS_ACTIVE_SAMPLER,
				60, 1, new Instrumentation.Variable<Long>() {
					public Long getValue() {
						return (long) executor.getActiveCount();
					}
				});
	}

	/**
	 * Get the list of strings of queue dump
	 * 
	 * @return the list of string that representing each CallableWrapper
	 */
	public List<String> getQueueDump() {
		List<String> list = new ArrayList<String>();
		for (QueueElement<CallableWrapper> qe : queue) {
			if (qe.toString() == null) {
				continue;
			}
			list.add(qe.toString());
		}
		return list;
	}

}
