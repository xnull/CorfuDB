package org.corfudb.test.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import javax.annotation.Nonnull;
import lombok.Data;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.test.parameters.Param;
import org.junit.jupiter.api.extension.ExtensionContext;

public class ConcurrentStateMachine {

    private final ExtensionContext extensionContext;
    private final List<IntConsumer> operations;
    private final Map<Integer, TestThread> threadMap;

    public ConcurrentStateMachine(@Nonnull ExtensionContext executionContext) {
        this.extensionContext = executionContext;
        this.operations = new ArrayList<>();
        this.threadMap = new HashMap<>();
    }

    public void addStep(@Nonnull IntConsumer operation) {
        operations.add(operation);
    }

    static class TestThread {
        @Data
        static class TestThreadTask {
            final Runnable task;
            final CompletableFuture<Void> future = new CompletableFuture<>();
        }

        final Thread thread;
        final BlockingQueue<TestThreadTask> tasks;
        volatile boolean shutdown = false;

        public TestThread(int num) {
            tasks = new LinkedBlockingQueue<>();
            thread = new Thread(this::process);
            thread.setName("test-" + num);
            thread.start();
        }

        public void shutdown() {
            shutdown = true;
            thread.interrupt();
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public CompletableFuture<Void> submitTask(Runnable task) {
            TestThreadTask thisTask = new TestThreadTask(task);
            tasks.add(thisTask);
            return thisTask.getFuture();
        }

        private void process() {
            try {
                while (!shutdown) {
                    TestThreadTask task = tasks.take();
                    try {
                        task.getTask().run();
                        task.getFuture().complete(null);
                    } catch (Throwable t) {
                        task.getFuture().completeExceptionally(t);
                        throw t;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
    private void t(int threadNum, Runnable task) {
        threadMap.computeIfAbsent(threadNum, TestThread::new).submitTask(task).join();
    }

    private void shutdownAllThreads() {
        threadMap.values().forEach(TestThread::shutdown);
    }

    public void executeThreaded(int numTasks, int numThreads, Duration timeout) {
        ConcurrentScheduler scheduler = new ConcurrentScheduler(extensionContext);
        scheduler.schedule(numTasks, numTask -> {
            for (IntConsumer step : operations) {
                step.accept(numTask);
            }
        });
        scheduler.execute(numThreads, timeout);
    }

    public void executeInterleaved(int numTasks, int numThreads) {
        try {
            final int NOTASK = -1;

            int numStates = operations.size();
            Random r = new Random((long) Param.RANDOM_SEED.getValue());
            AtomicInteger nDone = new AtomicInteger(0);

            int[] onTask = new int[numThreads];
            Arrays.fill(onTask, NOTASK);

            int[] onState = new int[numThreads];
            AtomicInteger highTask = new AtomicInteger(0);

            while (nDone.get() < numTasks) {
                final int nextt = r.nextInt(numThreads);

                if (onTask[nextt] == NOTASK) {
                    int t = highTask.getAndIncrement();
                    if (t < numTasks) {
                        onTask[nextt] = t;
                        onState[nextt] = 0;
                    }
                }

                if (onTask[nextt] != NOTASK) {
                    // invoke the next state-machine step of thread 'nextt'
                    t(nextt, () -> {
                        operations.get(onState[nextt]).accept(onTask[nextt]);
                        if (++onState[nextt] >= numStates) {
                            onTask[nextt] = NOTASK;
                            nDone.getAndIncrement();
                        }
                    });
                }
            }
        } finally {
            shutdownAllThreads();
        }
    }
}
