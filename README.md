[![Java CI with Maven](https://github.com/Adrian-26-Isotope/CustomJavaThreadPool/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/Adrian-26-Isotope/CustomJavaThreadPool/actions/workflows/maven.yml)

# ElasticThreadPool

A flexible and efficient custom thread pool implementation in Java that provides variable thread management with configurable minimum and maximum thread counts, idle timeouts, and different task polling strategies.

## Features

- **Variable Thread Pool Size**: Configure minimum and maximum number of threads
- **Idle Thread Management**: Automatic thread termination after configurable idle time
- **Core vs Non-Core Workers**: Distinction between persistent core threads and scalable non-core threads
- **Custom Thread Factory Support**: Use your own thread factory or default to virtual threads
- **Per-State Polling Behavior**: Different task polling behavior for each thread pool state
- **Task Completion Tracking**: Monitor completed task counts across all workers
- **Standard ExecutorService Interface**: Implements `AbstractExecutorService` for compatibility
- **Configurable Exception Handling**: Task failures are reported through the worker thread's `Thread.UncaughtExceptionHandler`

## Motivation: why not `ThreadPoolExecutor`?

The JDK's `ThreadPoolExecutor` cannot buffer tasks *and* scale threads with the actual backlog. Its
sizing policy is hard-coded in `execute()` and triggered by **queue rejection**, not by load:

```java
if (workerCount < corePoolSize)      addWorker(command, true);  // 1: fill core
else if (workQueue.offer(command))   ;                          // 2: queued — and that's all
else if (!addWorker(command, false)) reject(command);           // 3: only if the queue refused
```

A thread is created only when the queue *refuses* a task. That leads to:

| Queue | Behavior |
|-------|----------|
| `LinkedBlockingQueue` (unbounded) | `offer()` never fails → step 3 is unreachable. **`maximumPoolSize` is silently ignored**; the pool never exceeds `corePoolSize`. |
| `LinkedBlockingQueue(capacity)` | Grows only once the queue is *completely full* — scale-up as a last resort, after latency has built up. |
| `SynchronousQueue` (`newCachedThreadPool`) | Immediate scale-up, but **no buffering at all**. |

No configuration gives demand-proportional scaling with buffering, and the policy is not pluggable.
The known workaround — Apache Tomcat's `TaskQueue`, which overrides `offer()` to falsely report
"full" so step 3 fires, then re-queues on the resulting `RejectedExecutionException` — was rejected
here because it relies on undocumented `ThreadPoolExecutor` internals.

### What this pool does instead

Scale-up is driven by a measured backlog signal, `workerDemand` (`pendingTasks − idleWorkers`),
rather than by queue rejection:

```java
while ((workerDemand.get() > 0) && (workers.size() < maxThreads)) {
    startWorker(false);
}
```

If work is waiting and nobody is free, a worker appears — while the queue keeps buffering. The
decision runs on the dedicated `WorkerAdjuster` thread, so `execute()` returns immediately and
concurrent submitters aren't serialized behind the sizing logic.

### Comparison

| Aspect | `ThreadPoolExecutor` | `ElasticThreadPool` |
|--------|----------------------|--------------------|
| Scale-up trigger | Queue `offer()` returns `false` | `workerDemand > 0` (real backlog) |
| Scale-up with unbounded queue | **Never happens** | Works normally |
| Buffering + elasticity together | Not expressible | Yes |
| Where sizing runs | Caller thread, under `mainLock` | Dedicated `WorkerAdjuster` thread |
| Sizing policy location | Hard-coded in `execute()` | Isolated in `performAdjustment()` |
| Default threads | Platform | Virtual |
| Rejection policy | 4 pluggable handlers | Fixed `RejectedExecutionException` |
| Bounded queue / backpressure | Yes | Yes — bounded by default (capacity `10,000`) |
| Execution hooks | `beforeExecute` / `afterExecute` | None |
| Metrics | `getPoolSize`, `getActiveCount`, … | `getCompletedTasksCount()` only |
| Pluggable queue | Yes | Yes — `setQueue(BlockingQueue<Runnable>)` |

With the default virtual-thread factory, thread creation is cheap enough that reuse isn't the point
— in that mode this is best understood as an **elastic concurrency limiter with idle-based decay**,
which neither `ThreadPoolExecutor` nor `Executors.newVirtualThreadPerTaskExecutor()` plus a
semaphore provides.

Unlike Jetty's `QueuedThreadPool` (which is tightly coupled to Jetty internals such as leased/reserved threads and adaptive execution modes), this project stays a small, generic `ExecutorService` focused on backlog-driven scaling.

## Quick Start

### Basic Usage

```java
import org.adrian.threadpool.ElasticThreadPool;

// Create a thread pool with default settings
ElasticThreadPool threadPool = ElasticThreadPool.builder().start();

// Submit a task
threadPool.submit(() -> {
    System.out.println("Task executed by: " + Thread.currentThread().getName());
});

// Shutdown when done
threadPool.shutdown();
```

### Advanced Usage

```java
import java.time.Duration;
import java.util.concurrent.ThreadFactory;

ElasticThreadPool threadPool = ElasticThreadPool.builder()
    .setName("MyWorker")                               // Name prefix for worker threads
    .setMinThreads(2)                                  // Minimum 2 core threads
    .setMaxThreads(10)                                 // Maximum 10 threads
    .setIdleTime(Duration.ofSeconds(30))               // Idle timeout of 30 seconds
    .setThreadFactory(Thread.ofPlatform().factory())   // Use platform threads
    .start();

// Submit multiple tasks
for (int i = 0; i < 100; i++) {
    final int taskId = i;
    threadPool.submit(() -> {
        System.out.println("Processing task " + taskId);
        // Your task logic here
    });
}

// Monitor progress
System.out.println("Completed tasks: " + threadPool.getCompletedTasksCount());

threadPool.shutdown();
```

## Configuration Options

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `name` | JVM default | Name prefix for worker threads |
| `minThreads` | `0` | Minimum number of core threads that persist even when idle |
| `maxThreads` | `256` | Maximum number of threads that can be created |
| `idleTime` | `1 second` | Time after which idle non-core threads are terminated. Only governs non-core workers; core threads (up to `minThreads`) always wait indefinitely for the next task, regardless of `idleTime`, including `Duration.ZERO` |
| `threadFactory` | `Thread.ofVirtual().factory()` | Factory for creating new threads |
| `queue` | bounded `LinkedBlockingQueue` (capacity `10,000`) | Task queue used to buffer submitted tasks. Supply your own via `setQueue(BlockingQueue<Runnable>)` for a different capacity (e.g. `new LinkedBlockingQueue<>(5_000)`), an unbounded queue, or a different implementation entirely. |

## Architecture

### Core Components

- **`ElasticThreadPool`**: Main thread pool implementation extending `AbstractExecutorService`
- **`Worker`**: Individual worker threads that execute tasks
- **`WorkerAdjuster`**: Dedicated background thread that reacts to task submissions by
  performing worker count adjustments asynchronously, decoupling that work from the
  calling thread
- **`ThreadPoolState`**: Enum representing the pool's lifecycle state, where each
  constant also defines its own task polling behavior:
  - `RUNNING`: Core workers block indefinitely for the next task (`take()`); non-core workers block up to
    `idleTime` before giving up (`poll(idleTime)`)
  - `SHUTDOWN`: Workers drain remaining tasks without blocking
  - `NOT_RUNNING`: Workers always receive `null` (no more tasks to process)

### Thread Management

The thread pool distinguishes between two types of workers:

1. **Core Workers**: Created up to `minThreads` count, persist even when idle
2. **Non-Core Workers**: Created on-demand up to `maxThreads`, terminated after idle timeout

### Exception Handling

If a submitted task throws an exception, it is passed to the worker thread's
`Thread.getUncaughtExceptionHandler()` — the same mechanism used for any
uncaught exception in a `Thread`. Set a custom handler on your `ThreadFactory`
to observe or log task failures:

```java
ThreadFactory factory = Thread.ofVirtual()
    .uncaughtExceptionHandler((thread, ex) -> log.error("Task failed on " + thread.getName(), ex))
    .factory();

ElasticThreadPool pool = ElasticThreadPool.builder()
    .setThreadFactory(factory)
    .build();
```

Without a custom handler, the JVM's default behavior applies (printing the
thread name and stack trace to `System.err`). Either way, the failing worker
continues processing further tasks.

### Thread States and Lifecycle

1. **NOT_RUNNING**: Initial state and state after termination
2. **RUNNING**: Active state processing tasks
3. **SHUTDOWN**: Graceful shutdown in progress

The thread pool can be manually controlled using `start()`, `shutdown()`, and `shutdownNow()` methods. The builder comes with `build()` & `start()` to construct or construct & start the pool.

## Examples

```java
ElasticThreadPool pool = ElasticThreadPool.builder()
    .setMinThreads(Runtime.getRuntime().availableProcessors())
    .setMaxThreads(Runtime.getRuntime().availableProcessors())
    .setThreadFactory(Thread.ofPlatform().factory())
    .build();
pool.start();

// Submit CPU-bound tasks
for (int i = 0; i < 1000; i++) {
    pool.submit(() -> {
        // simulate CPU-intensive computation
        double result = Math.sqrt(Math.random() * 1000000);
    });
}
```

```java
ElasticThreadPool pool = ElasticThreadPool.builder()
    .setMinThreads(0)
    .setMaxThreads(1000)
    .setIdleTime(Duration.ofSeconds(5))
    .setThreadFactory(Thread.ofVirtual().factory()) // Default
    .start();

for (int i = 0; i < 10000; i++) {
    pool.submit(() -> {
        try {
            // Simulate I/O operation
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    });
}
```

See [ElasticThreadPoolTest.java](src-test/org/adrian/threadpool/ElasticThreadPoolTest.java) for more examples.

## Requirements

- Java 25+
- JUnit 5

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Adrian-26-Isotope
