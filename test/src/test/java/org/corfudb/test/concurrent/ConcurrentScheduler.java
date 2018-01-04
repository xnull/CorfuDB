package org.corfudb.test.concurrent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import jnr.ffi.annotations.In;
import lombok.Getter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.runners.model.MultipleFailureException;

public class ConcurrentScheduler {

    final ExtensionContext extensionContext;
    final List<Callable<Void>> operations;

    public ConcurrentScheduler(@Nonnull ExtensionContext executionContext) {
        this.extensionContext = executionContext;
        this.operations = new ArrayList<>();
    }

    public void schedule(int iterations, @Nonnull IntConsumer operation) {
        for (int i = 0; i < iterations; i++) {
            final int iteration = i;
            operations.add(() -> { operation.accept(iteration); return null; });
        }
    }

    public static class SchedulerException extends RuntimeException {
        @Getter
        Map<Integer, Throwable> exceptionMap;

        public SchedulerException(Map<Integer, Throwable> exceptionMap) {
            super(exceptionMap.entrySet().stream()
                        .map(e -> "ThreadParameter " + e.getKey() + ": "
                            + e.getValue().getClass().getSimpleName())
                        .collect(Collectors.joining("\n")));
            this.exceptionMap = exceptionMap;
        }
    }

    public void execute(int concurrency, @Nonnull Duration timeout) {
        ExecutorService service = Executors.newFixedThreadPool(concurrency,
            new ThreadFactoryBuilder()
                    .setDaemon(true)
                    .setNameFormat("test-%d")
                    .build());
        try {
            List<Future<Void>> futures = service.invokeAll(operations,
                timeout.toMillis(), TimeUnit.MILLISECONDS);
            service.shutdown();
            // All the futures should be completed at this point
            // Any exceptionally completed ones will throw an exception on get.
            Map<Integer, Throwable> exceptionMap = new HashMap<>();
            IntStream.range(0, futures.size())
                .forEach(i -> {
                    try {
                        futures.get(i).get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException | CancellationException e) {
                        exceptionMap.put(i, e);
                    }
                });
            if (!exceptionMap.isEmpty()) {
                throw new SchedulerException(exceptionMap);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnrecoverableCorfuInterruptedError(e);
        } finally {
            // Last ditch attempt to fix any misbehaving threads.
            service.shutdownNow();
            operations.clear();
        }
    }
}
