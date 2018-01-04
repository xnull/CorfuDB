package org.corfudb.test.concurrent;

import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.parameters.Param;

public class CorfuTestThread<T, O> {

    @Getter
    private CorfuRuntime runtime;

    @Getter
    private T test;

    @Getter
    private O object;

    private Thread testThread;

    private final BlockingQueue<CorfuTestThreadEvent<Object>> taskQueue
        = new LinkedBlockingQueue<>();

    private volatile boolean shutdown = false;

    public interface InterruptibleSupplier<T> {
        T get() throws InterruptedException, Exception;
    }

    @RequiredArgsConstructor
    static class CorfuTestThreadEvent<T> {
        final InterruptibleSupplier<T> supplier;
        final CompletableFuture<T> result = new CompletableFuture<>();
    }

    public void initialize(@Nonnull String name, @Nonnull T test, @Nullable CorfuRuntime runtime,
        @Nullable O object) {
        this.test = test;
        this.runtime = runtime;
        this.object = object;

        testThread = new Thread(this::eventLoop);
        testThread.setName(name);
        testThread.setDaemon(true);
        testThread.start();
    }

    private void eventLoop() {
        while (!shutdown) {
            try {
                CorfuTestThreadEvent<Object> event = taskQueue.take();
                try {
                    event.result.complete(event.supplier.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Throwable t) {
                    event.result.completeExceptionally(t);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @SuppressWarnings("unchecked")
    public <R> R run(InterruptibleSupplier<R> supplier) {
        CorfuTestThreadEvent<R> event = new CorfuTestThreadEvent<>(supplier);
        taskQueue.add((CorfuTestThreadEvent<Object>)event);
        try {
            return event.result.join();
        } catch (CompletionException e) {
            throw (RuntimeException) e.getCause();
        }
    }

    public void shutdown() {
        shutdown = true;
        testThread.interrupt();
        try {
            testThread.join(Param.TIMEOUT_LONG.getValue(Duration.class).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
