package adrian.os.java.threadpool;

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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This thread pool implementation uses a variable amount of threads to process submitted tasks.
 */
public class CustomThreadPool extends AbstractExecutorService {

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
     * net demand for workers, i.e. {@code pendingTasks - idleWorkers}, maintained incrementally. Each event below
     * applies exactly one atomic increment/decrement:
     * <ul>
     * <li>event A, {@code +1}: a task is enqueued ({@link #offer2Queue(Runnable)}): one more task is waiting for a
     * worker.</li>
     * <li>event D, {@code -1}: a worker is newly started ({@link #startWorker(boolean)}): it begins idle, adding
     * capacity.</li>
     * <li>event C, {@code -1}: a worker finishes a task and goes idle again ({@link #onWorkerIdle()}, called from
     * {@link Worker}): it becomes available again, adding capacity.</li>
     * <li>event E, {@code +1}: a worker terminates ({@link #stopWorker(Worker)}): idle capacity is removed again.
     * Unconditional - event F (a worker terminating while still busy) never happens with this {@link Worker}
     * implementation, since its run loop only ever exits in the idle state (see {@link #stopWorker(Worker)}).</li>
     * <li>event G, {@code -N}: {@code N} queued tasks are discarded unclaimed ({@link #shutdownNow()} draining the
     * queue): demand raised for them at enqueue time must be withdrawn since no worker will ever claim them.</li>
     * </ul>
     * Event B, a worker successfully claiming/dequeuing a task ({@code Worker.getTask()}), is deliberately NOT one of
     * those events: the task leaving the queue and that worker leaving the idle pool happen together, so their
     * contributions cancel out (net zero).
     */
    protected final AtomicInteger workerDemand = new AtomicInteger(0);

    /**
     * @return a {@link CustomThreadPool} builder.<br>
     *         Default values are:<br>
     *         - name = JVM default<br>
     *         - minimum threads = 0<br>
     *         - maximum threads = {@link Integer#MAX_VALUE}<br>
     *         - thread idle time = {@code Duration.ofSeconds(10)}<br>
     *         - thread factory ={@code Thread.ofVirtual().factory()}<br>
     */
    public static CustomThreadPoolBuilder builder() {
        return new CustomThreadPoolBuilder();
    }

    /**
     * Constructs a new thread pool with the given parameters. Needs to be started with {@link #start()} before it can
     * be used.
     *
     * @param minThreads the minimum amount of threads that shall not be terminated if idle. Must be greater than or
     *            equal to 0 and less than or equal to maxThreads.
     * @param maxThreads the maximum amount of threads created if enough tasks are submitted. Must be greater than 0
     *            and greater than or equal to minThreads.
     * @param idleDuration the duration after which threads will be terminated. Negative durations will be treated as
     *            {@code Duration.ZERO}.
     * @param threadFact a factory to define thread creation.
     */
    public CustomThreadPool(final int minThreads, final int maxThreads, final Duration idleDuration,
            final ThreadFactory threadFact) {
        if ((minThreads < 0) || (maxThreads < 1) || (minThreads > maxThreads)) {
            throw new IllegalArgumentException("invalid min/max threads: min=" + minThreads + ", max=" + maxThreads);
        }
        setState(ThreadPoolState.NOT_RUNNING);
        this.minThreads = minThreads;
        this.maxThreads = maxThreads;
        this.idleTime = idleDuration.isNegative() ? Duration.ZERO : idleDuration;
        this.threadFactory = threadFact;
        this.tasks = new LinkedBlockingQueue<>();
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
                // event A (see workerDemand javadoc): a pending task raises demand for a worker.
                this.workerDemand.incrementAndGet();
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
        // event D (see workerDemand javadoc): a freshly started worker begins idle.
        this.workerDemand.decrementAndGet();
        worker.getThread().start();
        return worker;
    }

    /**
     * called by a {@link Worker} once it goes idle again after completing a task (event C, see {@link #workerDemand}
     * javadoc).
     */
    void onWorkerIdle() {
        this.workerDemand.decrementAndGet();
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
        // event E (see workerDemand javadoc): a terminating worker leaving removes idle capacity, raising demand
        // back up. Unconditional because Worker.run()'s loop only ever exits right after onWorkerIdle() has already
        // run (or before ever claiming a task), i.e. always in the idle state - event F (a busy worker terminating)
        // never actually happens with this Worker implementation.
        this.workerDemand.incrementAndGet();
        worker.getThread().interrupt();
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
            // event G (see workerDemand javadoc): drained tasks were never claimed by a worker, so demand raised for
            // them at enqueue time must be withdrawn now that they're gone from the queue.
            this.workerDemand.addAndGet(-unfinishedTask.size());

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
     * builder for {@link CustomThreadPool}.
     */
    public static class CustomThreadPoolBuilder {

        private String name = "";
        private int minThreads = 0;
        private int maxThreads = Integer.MAX_VALUE;
        private Duration idleDuration = Duration.ofSeconds(10);
        private ThreadFactory threadFactory;

        /**
         * Set the name for the worker threads.<br>
         * <br>
         * Default is JVM name.
         */
        public CustomThreadPoolBuilder setName(final String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the minimum amount of threads.<br>
         * <br>
         * Default is 0.
         */
        public CustomThreadPoolBuilder setMinThreads(final int min) {
            this.minThreads = min;
            return this;
        }

        /**
         * Set the maximum amount of threads.<br>
         * <br>
         * Default is {@link Integer#MAX_VALUE}.
         */
        public CustomThreadPoolBuilder setMaxThreads(final int max) {
            this.maxThreads = max;
            return this;
        }

        /**
         * Set the idle time after its expiration a thread will be terminated.<br>
         * <br>
         * Default is 10 seconds.
         */
        public CustomThreadPoolBuilder setIdleTime(final Duration idleTime) {
            this.idleDuration = idleTime;
            return this;
        }

        /**
         * Set the factory to create new threads.<br>
         * <br>
         * {@code Thread.ofVirtual().factory()} is the default factory.
         */
        public CustomThreadPoolBuilder setThreadFactory(final ThreadFactory factory) {
            this.threadFactory = factory;
            return this;
        }

        /**
         * @return the newly constructed, un-started {@link CustomThreadPool}.
         */
        public CustomThreadPool build() {
            if (this.threadFactory == null) {
                if ((this.name != null) && !this.name.isBlank()) {
                    this.threadFactory = Thread.ofVirtual().name(this.name + "#", 0).factory();
                }
                else {
                    this.threadFactory = Thread.ofVirtual().factory();
                }
            }
            return new CustomThreadPool(this.minThreads, this.maxThreads, this.idleDuration, this.threadFactory);
        }

        /**
         * @return the newly constructed, already started {@link CustomThreadPool}.
         */
        public CustomThreadPool start() {
            var pool = build();
            pool.start();
            return pool;
        }

    }

}
