package adrian.os.java.threadpool;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Worker to execute tasks for the {@link CustomThreadPool}.
 */
public class Worker implements Runnable {

    private final CustomThreadPool threadPool;
    private final boolean core;
    private final Thread thread;
    private final AtomicLong completedTasksCount = new AtomicLong(0);

    /**
     * @param customThreadPool the thread pool that manages this worker.
     * @param keepAlive true if this shall be a core worker and not terminate.
     */
    protected Worker(final CustomThreadPool customThreadPool, final boolean keepAlive) {
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
    protected CustomThreadPool getThreadPool() {
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
        try {
            Runnable task;
            while (!getThread().isInterrupted() && !getThreadPool().isTerminated() && ((task = getTask()) != null)) {
                runTask(task);
            }
        }
        finally {
            getThreadPool().stopWorker(this);
        }
    }

    private Runnable getTask() {
        // event B (see CustomThreadPool.workerDemand javadoc): this task leaving the queue and this worker leaving
        // the idle pool happen together, so their effect on workerDemand cancels out - no counter update needed here.
        return getThreadPool().pollTask(this);
    }

    private void runTask(final Runnable task) {
        try {
            task.run();
            this.completedTasksCount.incrementAndGet();
        }
        catch (Exception e) {
            handleTaskError(e);
        }
        finally {
            getThreadPool().onWorkerIdle();
        }
    }

    /**
     * this handles exceptions thrown by the tasks, by delegating to this worker's thread's
     * {@link Thread.UncaughtExceptionHandler}, the same pluggable mechanism used for uncaught exceptions elsewhere.
     * Callers can customize it via the {@link CustomThreadPool}'s {@link java.util.concurrent.ThreadFactory}.
     */
    protected void handleTaskError(final Exception exception) {
        getThread().getUncaughtExceptionHandler().uncaughtException(getThread(), exception);
    }

}
