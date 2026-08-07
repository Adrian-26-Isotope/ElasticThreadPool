package org.adrian.threadpool;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Worker to execute tasks for the {@link ElasticThreadPool}.
 */
public class Worker implements Runnable {

    private final ElasticThreadPool threadPool;
    private final boolean core;
    private final Thread thread;
    private final AtomicLong completedTasksCount = new AtomicLong(0);
    private final ReentrantLock runLock = new ReentrantLock();
    private volatile boolean started = false;

    /**
     * @param customThreadPool the thread pool that manages this worker.
     * @param keepAlive true if this shall be a core worker and not terminate.
     */
    protected Worker(final ElasticThreadPool customThreadPool, final boolean keepAlive) {
        this.threadPool = customThreadPool;
        this.core = keepAlive;
        this.thread = getThreadPool().getThreadFactory().newThread(this);
    }

    /**
     * get the underlying thread.
     */
    protected Thread getThread() {
        return this.thread;
    }

    /**
     * @return the thread pool that manages this worker.
     */
    protected ElasticThreadPool getThreadPool() {
        return this.threadPool;
    }

    /**
     * @return the number of completed task.
     */
    protected long getCompletedTasksCount() {
        return this.completedTasksCount.get();
    }

    /**
     * @return if this is a core worker.
     */
    protected boolean isCore() {
        return this.core;
    }

    @Override
    public void run() {
        // must be set before the first getTask() call: it's what tells interruptIfIdle() this worker has actually
        // started (as opposed to merely being added to ElasticThreadPool.workers and about to be started), so a
        // pending interrupt can never suppress this thread's very first task fetch (see interruptIfIdle()).
        this.started = true;
        try {
            Runnable task;
            // deliberately does NOT also check getThread().isInterrupted(): runLock is free (hence interruptible,
            // see interruptIfIdle()) for a brief moment right here too, between finishing one task and looping back
            // for the next, not just while genuinely blocked in getTask(). Bailing out on a stale interrupt flag
            // before even calling getTask() could strand an already-queued task that only this worker would ever
            // have picked up (e.g. once maxThreads is reached). getTask()/pollTask() already handle interruption
            // correctly on their own (see ThreadPoolState.RUNNING.pollTask()), so loop continuation only needs to
            // depend on the pool's state and on whether a task was actually obtained.
            while (!getThreadPool().isTerminated() && ((task = getTask()) != null)) {
                runTask(task);
            }
        }
        finally {
            getThreadPool().stopWorker(this);
        }
    }

    private Runnable getTask() {
        // event B (see WorkerDemand javadoc): this task leaving the queue and this worker leaving the idle pool
        // happen together, so their effect on workerDemand cancels out - no counter update needed here.
        return getThreadPool().pollTask(this);
    }

    private void runTask(final Runnable task) {
        this.runLock.lock();
        try {
            task.run();
            this.completedTasksCount.incrementAndGet();
        }
        catch (Exception e) {
            handleTaskError(e);
        }
        finally {
            this.runLock.unlock();
            getThreadPool().onWorkerIdle();
        }
    }

    /**
     * Interrupts this worker's thread only if it is not currently executing a task. {@link #runLock} is only held while
     * {@link #runTask(Runnable)} is executing, so successfully acquiring it here (without blocking) is proof this
     * worker is idle - either blocked in {@code take()}/{@code poll()}, or about to call one of them.<br>
     * <br>
     * {@link #started} additionally guards a worker that has been added to {@link ElasticThreadPool#getWorkers()} but
     * whose thread has not yet begun {@link #run()}: {@link #runLock} would still be free at that point too, but
     * interrupting it now would set the interrupt flag before {@link #run()}'s loop ever runs, causing it to exit
     * without ever calling {@link #getTask()} - silently abandoning the very task it was started to drain.
     */
    protected void interruptIfIdle() {
        if (this.started && this.runLock.tryLock()) {
            try {
                this.thread.interrupt();
            }
            finally {
                this.runLock.unlock();
            }
        }
    }

    /**
     * this handles exceptions thrown by the tasks, by delegating to this worker's thread's
     * {@link Thread.UncaughtExceptionHandler}, the same pluggable mechanism used for uncaught exceptions elsewhere.
     * Callers can customize it via the {@link ElasticThreadPool}'s {@link java.util.concurrent.ThreadFactory}.
     */
    protected void handleTaskError(final Exception exception) {
        getThread().getUncaughtExceptionHandler().uncaughtException(getThread(), exception);
    }

}
