package org.adrian.threadpool;

/**
 * Dedicated background worker that reacts to {@link ElasticThreadPool#execute(Runnable)} calls by running
 * {@link ElasticThreadPool#performAdjustment()} on its own thread, decoupling that (potentially expensive) work from the
 * thread calling {@code execute()}.
 */
class WorkerAdjuster implements Runnable {

    private final ElasticThreadPool threadPool;
    private final Thread thread;
    private final Object signalLock = new Object();
    private boolean dirty = false;

    protected WorkerAdjuster(final ElasticThreadPool customThreadPool) {
        this.threadPool = customThreadPool;
        this.thread = this.threadPool.getThreadFactory().newThread(this);
        this.thread.start();
    }

    /**
     * @return the underlying thread.
     */
    protected Thread getThread() {
        return this.thread;
    }

    /**
     * Signal that worker adjustment may be necessary (e.g. after a task was enqueued). Returns immediately; the actual
     * adjustment work happens asynchronously on this adjuster's own thread.
     */
    protected void signal() {
        synchronized (this.signalLock) {
            this.dirty = true;
            this.signalLock.notifyAll();
        }
    }

    /**
     * Wake this adjuster up so it can notice pool termination and stop, even if no adjustment is currently pending.
     */
    protected void wakeup() {
        synchronized (this.signalLock) {
            this.signalLock.notifyAll();
        }
    }

    @Override
    public void run() {
        while (true) {
            if (!waitTillDirty()) {
                return; // pool terminated --> exit
            }
            try {
                this.threadPool.performAdjustment();
            }
            catch (RuntimeException e) {
                // keep looping regardless, so a
                // bad adjustment never permanently kills this coordinator thread.
                this.thread.getUncaughtExceptionHandler().uncaughtException(this.thread, e);
            }
        }
    }

    private boolean waitTillDirty() {
        synchronized (this.signalLock) {
            while (!this.dirty) {
                if (this.threadPool.isTerminated()) {
                    return false;
                }
                try {
                    this.signalLock.wait();
                }
                catch (InterruptedException e) {
                    // termination is driven by pool state, not interruption; keep looping
                }
            }
            this.dirty = false;
            return true;
        }
    }

}
