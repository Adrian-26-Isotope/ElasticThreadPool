[![Java CI with Maven](https://github.com/Adrian-26-Isotope/CustomJavaThreadPool/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/Adrian-26-Isotope/CustomJavaThreadPool/actions/workflows/maven.yml)

# CustomThreadPool

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

## Quick Start

### Basic Usage

```java
import adrian.os.java.threadpool.CustomThreadPool;

// Create a thread pool with default settings
CustomThreadPool threadPool = CustomThreadPool.builder().start();

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

CustomThreadPool threadPool = CustomThreadPool.builder()
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
| `maxThreads` | `Integer.MAX_VALUE` | Maximum number of threads that can be created |
| `idleTime` | `10 seconds` | Time after which idle non-core threads are terminated. Only governs non-core workers; core threads (up to `minThreads`) always wait indefinitely for the next task, regardless of `idleTime`, including `Duration.ZERO` |
| `threadFactory` | `Thread.ofVirtual().factory()` | Factory for creating new threads |

## Architecture

### Core Components

- **`CustomThreadPool`**: Main thread pool implementation extending `AbstractExecutorService`
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

CustomThreadPool pool = CustomThreadPool.builder()
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
CustomThreadPool pool = CustomThreadPool.builder()
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
CustomThreadPool pool = CustomThreadPool.builder()
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

See [CustomThreadPoolTest.java](src-test/adrian/os/java/threadpool/CustomThreadPoolTest.java) for more examples.

## Requirements

- Java 25+
- JUnit 5

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

Adrian-26-Isotope
