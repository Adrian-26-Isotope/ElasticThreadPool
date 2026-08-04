package adrian.os.java.threadpool;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * State of a {@link CustomThreadPool}. Each constant also defines how a {@link Worker} polls for its next task while
 * the pool is in that state.<br>
 * <br>
 * This enum is deliberately decoupled from {@link Worker} and {@link CustomThreadPool}: it only ever sees a
 * {@link WorkerPollContext} snapshot, never the live objects themselves.
 */
enum ThreadPoolState {

    /** Active state processing tasks. */
    RUNNING {
        /**
         * {@inheritDoc}<br>
         * Only non core threads can return null tasks.
         */
        @Override
        public Runnable pollTask(final WorkerPollContext context) {
            Runnable task = null;
            Thread thread = context.thread();
            // a just-queued task is always polled at least once,
            // even if the pool state flips RUNNING->SHUTDOWN between dispatch
            // (getState().pollTask()) and this loop's isRunning() check - otherwise
            // that task is silently stranded forever (see checkTermination()).
            do {
                try {
                    task = context.tasks().poll(context.idleTime().toNanos(), TimeUnit.NANOSECONDS);
                }
                catch (InterruptedException _) {
                    thread.interrupt();
                }
                if ((task != null) || !context.core()) {
                    // core workers will continue polling if task is null
                    return task;
                }
            } while (!thread.isInterrupted() && context.runningCheck().getAsBoolean());
            return task;
        }
    },

    /** Initial state and state after termination. */
    NOT_RUNNING {
        /**
         * {@inheritDoc}<br>
         * It always returns null.
         */
        @Override
        public Runnable pollTask(final WorkerPollContext context) {
            return null;
        }
    },

    /** Graceful shutdown in progress. */
    SHUTDOWN {
        /**
         * {@inheritDoc}<br>
         * Core threads can also return null tasks, and thus be terminated as a consequence.
         */
        @Override
        public Runnable pollTask(final WorkerPollContext context) {
            return context.tasks().poll();
        }
    };

    /**
     * Immutable snapshot of exactly what a {@link ThreadPoolState} needs to poll the next task for a worker, decoupling
     * this state machine from the {@link Worker}/{@link CustomThreadPool} types themselves.
     *
     * @param tasks        the task queue to poll from.
     * @param idleTime     the duration a worker may block waiting for a task before giving up.
     * @param core         whether the polling worker is a core worker (kept alive despite a null poll while RUNNING).
     * @param thread       the polling worker's own thread, used to react to interruption.
     * @param runningCheck supplies whether the owning thread pool is still in the RUNNING state.
     */
    record WorkerPollContext(BlockingQueue<Runnable> tasks, Duration idleTime, boolean core, Thread thread,
            BooleanSupplier runningCheck) {
    }

    /**
     * The given context polls the next task to be processed, according to this state's polling behavior.
     *
     * @param context the polling worker's context.
     * @return the next task or null.
     */
    public abstract Runnable pollTask(WorkerPollContext context);
}
