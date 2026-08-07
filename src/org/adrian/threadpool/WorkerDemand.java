package org.adrian.threadpool;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks {@link ElasticThreadPool}'s net demand for workers, i.e. {@code pendingTasks - idleWorkers}, maintained
 * incrementally rather than ever being recomputed from scratch. Deliberately exposes no generic increment/decrement:
 * every mutation is a named event method below, so a call site can only record one of the events it was designed for,
 * not an arbitrary atomic op.<br>
 * <br>
 * Each event applies exactly one atomic increment/decrement:
 * <ul>
 * <li>event A, {@code +1}: a task is enqueued ({@link #recordTaskEnqueued()}): one more task is waiting for a
 * worker.</li>
 * <li>event D, {@code -1}: a worker is newly started ({@link #recordWorkerStarted()}): it begins idle, adding
 * capacity.</li>
 * <li>event C, {@code -1}: a worker finishes a task and goes idle again ({@link #recordWorkerIdle()}): it becomes
 * available again, adding capacity.</li>
 * <li>event E, {@code +1}: a worker terminates ({@link #recordWorkerStopped()}): idle capacity is removed again.
 * Unconditional - event F (a worker terminating while still busy) never happens with this {@link Worker}
 * implementation, since its run loop only ever exits in the idle state (see
 * {@code ElasticThreadPool.stopWorker()}).</li>
 * <li>event G, {@code -N}: {@code N} queued tasks are discarded unclaimed ({@link #recordTasksDiscarded(int)}, from
 * {@code ElasticThreadPool.shutdownNow()} draining the queue): demand raised for them at enqueue time must be withdrawn
 * since no worker will ever claim them.</li>
 * </ul>
 * Event B, a worker successfully claiming/dequeuing a task ({@code Worker.getTask()}), is deliberately NOT one of those
 * events: the task leaving the queue and that worker leaving the idle pool happen together, so their contributions
 * cancel out (net zero).
 */
final class WorkerDemand {

    private final AtomicInteger demand = new AtomicInteger(0);

    /**
     * @return the current net demand for workers ({@code pendingTasks - idleWorkers}).
     */
    int get() {
        return this.demand.get();
    }

    /**
     * records event A: a task was enqueued, raising demand for a worker to pick it up.
     */
    void recordTaskEnqueued() {
        this.demand.incrementAndGet();
    }

    /**
     * records event D: a worker was newly started and begins idle, adding capacity.
     */
    void recordWorkerStarted() {
        this.demand.decrementAndGet();
    }

    /**
     * records event C: a worker finished a task and went idle again, adding capacity.
     */
    void recordWorkerIdle() {
        this.demand.decrementAndGet();
    }

    /**
     * records event E: a worker terminated, removing the idle capacity it provided.
     */
    void recordWorkerStopped() {
        this.demand.incrementAndGet();
    }

    /**
     * records event G: {@code count} queued tasks were discarded unclaimed, withdrawing the demand raised for them at
     * enqueue time since no worker will ever claim them.
     *
     * @param count the number of discarded tasks.
     */
    void recordTasksDiscarded(final int count) {
        this.demand.addAndGet(-count);
    }
}
