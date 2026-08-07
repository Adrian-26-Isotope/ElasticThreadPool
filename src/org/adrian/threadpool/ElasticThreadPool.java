package org.adrian.threadpool;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This thread pool implementation uses a variable amount of threads to process submitted tasks.<br>
 * <br>
 * <b>Why not {@link java.util.concurrent.ThreadPoolExecutor}?</b><br>
 * This pool exists because {@code ThreadPoolExecutor} cannot express "buffer tasks <em>and</em> grow the worker count
 * in proportion to the actual backlog". Its sizing policy is hard-coded into {@code execute()} and is driven by
 * <em>queue rejection</em> rather than by load:
 *
 * <pre>
 * if (workerCount &lt; corePoolSize)      addWorker(command, true);  // 1: fill core
 * else if (workQueue.offer(command))   ;                          // 2: queued - and that is all
 * else if (!addWorker(command, false)) reject(command);           // 3: only if the queue refused
 * </pre>
 *
 * Step 2 is the decisive one: a new thread is created only once the queue <em>refuses</em> a task. The consequences
 * are:
 * <ul>
 * <li>with an unbounded queue, {@code offer()} never fails, so step 3 is unreachable and {@code maximumPoolSize} is
 * silently ignored - the pool never grows past {@code corePoolSize};</li>
 * <li>with a bounded queue, the pool only grows once the queue is completely full, i.e. scale-up is a last resort that
 * happens after latency has already accumulated;</li>
 * <li>with a {@code SynchronousQueue}, scale-up is immediate but there is no buffering at all.</li>
 * </ul>
 * No combination of those settings yields demand-proportional scaling with buffering, and the policy is not
 * pluggable.<br>
 * <br>
 * <b>The replacement policy</b><br>
 * Scale-up here is driven by {@link #workerDemand} ({@code pendingTasks - idleWorkers}), a measured backlog signal,
 * instead of by queue rejection: see {@link #performAdjustment()}. If work is waiting and no worker is free, a worker
 * is created, while the queue keeps buffering. That decision runs on a dedicated {@link WorkerAdjuster} thread rather
 * than the caller's, so {@link #execute(Runnable)} returns immediately and concurrent submitters are not serialized
 * behind the sizing logic.<br>
 * <br>
 * <b>Virtual threads</b><br>
 * The default thread factory produces virtual threads, where thread creation is cheap enough that reuse is not the
 * point. In that mode this class is best understood as an <em>elastic concurrency limiter</em> with idle-based decay -
 * something neither {@code ThreadPoolExecutor} nor {@code Executors.newVirtualThreadPerTaskExecutor()} combined with a
 * semaphore provides.
 */
public class ElasticThreadPool extends AbstractExecutorService {

    /**
     * default capacity of the task queue used when no queue is explicitly supplied (see
     * {@link ElasticThreadPoolBuilder#setQueue(BlockingQueue)}): {@value}.
     */
    public static final int DEFAULT_QUEUE_CAPACITY = 10_000;

    private final int minThreads;
    private final int maxThreads;
    private final Duration idleTime;
    private final ThreadFactory threadFactory;
    private final BlockingQueue<Runnable> tasks;
    private final List<Worker> workers;
    private volatile ThreadPoolState state;
    private WorkerAdjuster workerAdjuster;
    /**
     * task count of terminated workers
     */
    private final AtomicLong completedTasksCount = new AtomicLong(0);
    /**
     * net demand for workers, i.e. {@code pendingTasks - idleWorkers}, maintained incrementally by named event methods
     * - see {@link WorkerDemand} for the full event protocol (A-G).
     */
    private final WorkerDemand workerDemand = new WorkerDemand();

    /**
     * @return a {@link ElasticThreadPool} builder.
     */
    public static ElasticThreadPoolBuilder builder() {
        return new ElasticThreadPoolBuilder();
    }

    /**
     * Constructs a new thread pool with the given parameters and a caller-supplied task queue. Needs to be started with
     * {@link #start()} before it can be used.
     *
     * @param minThreads the minimum amount of threads that shall not be terminated if idle. Must be greater than or
     *            equal to 0 and less than or equal to maxThreads.
     * @param maxThreads the maximum amount of threads created if enough tasks are submitted. Must be greater than 0
     *            and greater than or equal to minThreads.
     * @param idleDuration the duration after which threads will be terminated. Negative durations will be treated as
     *                     {@code Duration.ZERO}.
     * @param threadFact   a factory to define thread creation.
     * @param queue        the queue used to buffer submitted tasks before a worker picks them up. Callers may supply a
     *                     bounded queue of any capacity, an unbounded queue, or any other {@link BlockingQueue}
     *                     implementation (e.g. {@code PriorityBlockingQueue}). Must not be {@code null}.
     */
    public ElasticThreadPool(final int minThreads, final int maxThreads, final Duration idleDuration,
            final ThreadFactory threadFact, final BlockingQueue<Runnable> queue) {
        if ((minThreads < 0) || (maxThreads < 1) || (minThreads > maxThreads)) {
            throw new IllegalArgumentException("invalid min/max threads: min=" + minThreads + ", max=" + maxThreads);
        }
        setState(ThreadPoolState.NOT_RUNNING);
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.idleTime = idleDuration.isNegative() ? Duration.ZERO : idleDuration;
        this.threadFactory = threadFact;
        this.tasks = Objects.requireNonNull(queue, "queue must not be null");
        this.workers = Collections.synchronizedList(new ArrayList<>(minThreads));
    }

    /**
     * @return the state of this thread pool.
     */
    protected ThreadPoolState getState() {
        return this.state;
    }

    /**
     * @return idle duration after that worker threads will be terminated.
     */
    public Duration getIdleTime() {
        return this.idleTime;
    }

    /**
     * @return the factory to create new threads.
     */
    protected ThreadFactory getThreadFactory() {
        return this.threadFactory;
    }

    /**
     * @return a current snapshot of waiting tasks.
     */
    protected BlockingQueue<Runnable> getTasks() {
        return this.tasks;
    }

    /**
     * @return a current snapshot of the worker threads.
     */
    protected List<Worker> getWorkers() {
        return this.workers;
    }

    /**
     * @return the current worker demand ({@code pendingTasks - idleWorkers}).
     */
    int getWorkerDemand() {
        return this.workerDemand.get();
    }

    /**
     * @return number of completed tasks by this thread pool.
     */
    public long getCompletedTasksCount() {
        // reads the accumulator inside the same critical section used to scan
        // the live workers, so the result is atomic relative to stopWorker().
        synchronized (this.workers) {
            long count = this.completedTasksCount.get();
            for (Worker worker : this.workers) {
                count += worker.getCompletedTasksCount();
            }
            return count;
        }
    }

    /**
     * start this thread pool, if not already started. Is automatically started at construction.
     */
    public synchronized void start() {
        if (getState() == ThreadPoolState.NOT_RUNNING) {
            setState(ThreadPoolState.RUNNING);
            for (int i = 0; i < this.minThreads; i++) {
                startWorker(true);
            }
            this.workerAdjuster = new WorkerAdjuster(this);
        }
    }

    @Override
    public void execute(final Runnable command) {
        if (offer2Queue(Objects.requireNonNull(command))) {
            adjustWorkers();
        }
        else {
            throw new RejectedExecutionException("Task " + command + " rejected: thread pool is not RUNNING");
        }
    }

    private synchronized boolean offer2Queue(final Runnable task) {
        // synchronized on `this` so this check-then-act cannot interleave with
        // shutdown()/checkTermination()'s own check-then-act: a task can never be
        // enqueued after (or concurrently with) the pool declaring itself terminated.
        if (getState() == ThreadPoolState.RUNNING) {
            boolean offered = this.tasks.offer(task);
            if (offered) {
                // event A (see WorkerDemand javadoc): a pending task raises demand for a worker.
                this.workerDemand.recordTaskEnqueued();
            }
            return offered;
        }
        return false;
    }

    /**
     * signal that worker adjustment may be necessary (e.g. after a task was enqueued). Returns immediately; the actual
     * work ({@link #performAdjustment()}) happens asynchronously on the {@link WorkerAdjuster}'s own thread, so this no
     * longer serializes concurrent {@link #execute(Runnable)} calls behind a single lock.
     */
    private void adjustWorkers() {
        this.workerAdjuster.signal();
    }

    /**
     * if necessary start new workers.<br>
     * <br>
     * Deliberately does NOT check the pool state: {@link #execute(Runnable)} always triggers this (via
     * {@link #adjustWorkers()}) right after a successful {@link #offer2Queue(Runnable)}, even during {@code SHUTDOWN},
     * so that a just-enqueued task is guaranteed to get a worker that can drain it (see
     * {@link ThreadPoolState#SHUTDOWN}). If this method is ever changed to skip starting workers while not
     * {@code RUNNING}, a task could be left stranded in {@link #tasks} with no worker left to pick it up, and
     * {@link #checkTermination()} would then never be satisfied, hanging {@link #awaitTermination(long, TimeUnit)}
     * forever.<br>
     * <br>
     * Only ever called by this pool's {@link WorkerAdjuster} thread, one call at a time, so re-reading
     * {@link #workerDemand} fresh on every loop iteration is race-free: {@code workerDemand} is kept consistent by
     * single atomic increments/decrements at the events listed in its javadoc.
     */
    protected void performAdjustment() {
        while ((this.workerDemand.get() > 0) && (this.workers.size() < this.maxThreads)) {
            startWorker(false);
        }
    }

    private Worker startWorker(final boolean keepAlive) {
        Worker worker = new Worker(this, keepAlive);
        this.workers.add(worker);
        // event D (see WorkerDemand javadoc): a freshly started worker begins idle.
        this.workerDemand.recordWorkerStarted();
        worker.getThread().start();
        return worker;
    }

    /**
     * called by a {@link Worker} once it goes idle again after completing a task (event C, see {@link WorkerDemand}
     * javadoc).
     */
    void onWorkerIdle() {
        this.workerDemand.recordWorkerIdle();
    }

    /**
     * Terminate the given worker and remove it from the thread pool.
     */
    protected void stopWorker(final Worker worker) {
        // a concurrent reader can never observe the worker counted in both the
        // list scan AND the accumulator, nor in neither (see getCompletedTasksCount()).
        synchronized (this.workers) {
            this.workers.remove(worker);
            this.completedTasksCount.addAndGet(worker.getCompletedTasksCount());
        }
        // event E (see WorkerDemand javadoc): a terminating worker leaving removes idle capacity, raising demand
        // back up. Unconditional because Worker.run()'s loop only ever exits right after onWorkerIdle() has already
        // run (or before ever claiming a task), i.e. always in the idle state - event F (a busy worker terminating)
        // never actually happens with this Worker implementation.
        this.workerDemand.recordWorkerStopped();
        checkTermination();
    }

    private synchronized void checkTermination() {
        // tasks.isEmpty() must hold too: otherwise a task offered while RUNNING
        // (see offer2Queue()) could be stranded in the queue with no worker left
        // to pick it up, while the pool incorrectly reports itself terminated.
        // This relies on performAdjustment() always being triggered (state-independent)
        // right after a successful enqueue to actually drain that task; see the
        // warning on performAdjustment() before changing that behavior.
        if (this.workers.isEmpty() && this.tasks.isEmpty() && (getState() == ThreadPoolState.SHUTDOWN)) {
            setState(ThreadPoolState.NOT_RUNNING);
            synchronized (this.workers) {
                this.workers.notifyAll(); // notify termination complete
            }
            this.workerAdjuster.wakeup(); // let the coordinator notice termination and stop
        }
    }

    /**
     * set the thread pool state. The polling behavior for workers follows directly from
     * {@link ThreadPoolState#pollTask(ThreadPoolState.WorkerPollContext)}.
     */
    protected synchronized void setState(final ThreadPoolState state) {
        this.state = state;
    }

    @Override
    public synchronized void shutdown() {
        // workers will pick up remaining tasks. no new tasks can be submitted.
        // synchronized on `this`, matching offer2Queue(), so a task cannot be
        // enqueued and then "missed" by the termination check below.
        if (getState() == ThreadPoolState.RUNNING) {
            setState(ThreadPoolState.SHUTDOWN);
            // wake idle workers promptly instead of waiting out idleTime (see Worker.interruptIfIdle()).
            synchronized (this.workers) {
                for (Worker worker : this.workers) {
                    worker.interruptIfIdle();
                }
            }
            checkTermination();
        }
    }

    @Override
    public synchronized List<Runnable> shutdownNow() {
        List<Runnable> unfinishedTask = new ArrayList<>();
        if (getState() != ThreadPoolState.NOT_RUNNING) {
            // 1st: initiate orderly shutdown
            shutdown();

            // 2nd: cancel all pending tasks
            this.tasks.drainTo(unfinishedTask);
            // event G (see WorkerDemand javadoc): drained tasks were never claimed by a worker, so demand raised for
            // them at enqueue time must be withdrawn now that they're gone from the queue.
            this.workerDemand.recordTasksDiscarded(unfinishedTask.size());

            // 3rd: interrupt all running threads
            synchronized (this.workers) {
                for (Worker worker : this.workers) {
                    worker.getThread().interrupt();
                }
            }

            // draining the queue above can make tasks.isEmpty() newly true;
            // if there were no workers left to pick up those drained tasks (e.g. they
            // were never started), no worker will ever call stopWorker()->checkTermination()
            // again, so re-check here or the pool is stuck in SHUTDOWN forever.
            checkTermination();
        }
        return unfinishedTask;
    }

    @Override
    public boolean awaitTermination(final long timeout, final TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        synchronized (this.workers) {
            while (getState() != ThreadPoolState.NOT_RUNNING) {
                if (remainingNanos <= 0) {
                    return false;
                }
                long waitStart = System.nanoTime();
                long millis = remainingNanos / 1_000_000L;
                int nanos = (int) (remainingNanos % 1_000_000L);
                this.workers.wait(millis, nanos);
                remainingNanos -= System.nanoTime() - waitStart;
            }
        }
        return true;
    }

    /**
     * Returns true if this executor is running.
     */
    public boolean isRunning() {
        return getState() == ThreadPoolState.RUNNING;
    }

    @Override
    public boolean isShutdown() {
        return getState() == ThreadPoolState.SHUTDOWN;
    }

    @Override
    public boolean isTerminated() {
        return getState() == ThreadPoolState.NOT_RUNNING;
    }

    protected Runnable pollTask(final Worker worker) {
        var context = new ThreadPoolState.WorkerPollContext(this.tasks, this.idleTime, worker.isCore(),
                worker.getThread());
        return getState().pollTask(context);
    }

    /**
     * builder for {@link ElasticThreadPool}.
     */
    public static class ElasticThreadPoolBuilder {

        private String name = "";
        private int minThreads = 0;
        private int maxThreads = 256;
        private Duration idleDuration = Duration.ofSeconds(1);
        private ThreadFactory threadFactory;
        private BlockingQueue<Runnable> queue;

        /**
         * Set the name for the worker threads.<br>
         * <br>
         * Default is JVM name.
         */
        public ElasticThreadPoolBuilder setName(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the minimum amount of threads.<br>
         * <br>
         * Default is 0.
         */
        public ElasticThreadPoolBuilder setMinThreads(final int min) {
            this.minThreads = min;
            return this;
        }

        /**
         * Set the maximum amount of threads.<br>
         * <br>
         * Default is 256.
         */
        public ElasticThreadPoolBuilder setMaxThreads(final int max) {
            this.maxThreads = max;
            return this;
        }

        /**
         * Set the idle time after its expiration a thread will be terminated.<br>
         * <br>
         * Default is 1 seconds.
         */
        public ElasticThreadPoolBuilder setIdleTime(final Duration idleTime) {
            this.idleDuration = idleTime;
            return this;
        }

        /**
         * Set the factory to create new threads.<br>
         * <br>
         * {@code Thread.ofVirtual().factory()} is the default factory.
         */
        public ElasticThreadPoolBuilder setThreadFactory(final ThreadFactory factory) {
            this.threadFactory = factory;
            return this;
        }

        /**
         * Set the queue used to buffer submitted tasks before a worker picks them up. Use this to change the default
         * capacity (e.g. {@code new LinkedBlockingQueue<>(5_000)}), use an unbounded queue, or plug in a different
         * {@link BlockingQueue} implementation entirely.<br>
         * <br>
         * Default is a bounded {@link LinkedBlockingQueue} with capacity
         * {@link ElasticThreadPool#DEFAULT_QUEUE_CAPACITY}. Passing {@code null} resets to that default.
         *
         * @param queue the task queue to use, or {@code null} to reset to the default bounded queue.
         */
        public ElasticThreadPoolBuilder setQueue(final BlockingQueue<Runnable> queue) {
            this.queue = queue;
            return this;
        }

        /**
         * @return the newly constructed, un-started {@link ElasticThreadPool}.
         */
        public ElasticThreadPool build() {
            if (this.threadFactory == null) {
                if ((this.name != null) && !this.name.isBlank()) {
                    this.threadFactory = Thread.ofVirtual().name(this.name + "#", 0).factory();
                }
                else {
                    this.threadFactory = Thread.ofVirtual().factory();
                }
            }
            BlockingQueue<Runnable> effectiveQueue = this.queue != null ? this.queue
                    : new LinkedBlockingQueue<>(ElasticThreadPool.DEFAULT_QUEUE_CAPACITY);
            return new ElasticThreadPool(this.minThreads, this.maxThreads, this.idleDuration, this.threadFactory,
                    effectiveQueue);
        }

        /**
         * @return the newly constructed, already started {@link ElasticThreadPool}.
         */
        public ElasticThreadPool start() {
            var pool = build();
            pool.start();
            return pool;
        }

    }

}
