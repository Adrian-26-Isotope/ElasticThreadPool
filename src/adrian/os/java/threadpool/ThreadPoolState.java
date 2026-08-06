package adrian.os.java.threadpool;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
         * Core workers block indefinitely for the next task; non core workers give up (returning null) after
         * {@code idleTime}. Either way, an interrupt (e.g. from {@link Worker#interruptIfIdle()}) never discards a task
         * that is already sitting in the queue: since {@code take()}/{@code poll(timeout, unit)} throw
         * {@link InterruptedException} immediately if the calling thread is already interrupted - even when a task is
         * available and would otherwise have been returned without any actual waiting - a final non-blocking
         * {@code poll()} is attempted before giving up, exactly like the (already interrupt-agnostic)
         * {@link ThreadPoolState#SHUTDOWN} behavior below.
         */
        @Override
        public Runnable pollTask(final WorkerPollContext context) {
            Thread thread = context.thread();
            if (context.core()) {
                try {
                    return context.tasks().take();
                }
                catch (InterruptedException _) {
                    thread.interrupt();
                    return context.tasks().poll();
                }
            }

            try {
                return context.tasks().poll(context.idleTime().toNanos(), TimeUnit.NANOSECONDS);
            }
            catch (InterruptedException _) {
                thread.interrupt();
                return context.tasks().poll();
            }
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
     * @param tasks    the task queue to poll from.
     * @param idleTime the duration a non core worker may block waiting for a task before giving up.
     * @param core     whether the polling worker is a core worker (blocks indefinitely for a task while RUNNING).
     * @param thread   the polling worker's own thread, used to react to interruption.
     */
    record WorkerPollContext(BlockingQueue<Runnable> tasks, Duration idleTime, boolean core, Thread thread) {
    }

    /**
     * The given context polls the next task to be processed, according to this state's polling behavior.
     *
     * @param context the polling worker's context.
     * @return the next task or null.
     */
    public abstract Runnable pollTask(WorkerPollContext context);
}
