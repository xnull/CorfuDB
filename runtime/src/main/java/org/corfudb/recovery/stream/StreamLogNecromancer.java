package org.corfudb.recovery.stream;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.RecoveryException;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@Slf4j
public class StreamLogNecromancer {
    // In charge of summoning Corfu maps back in this world
    private final ExecutorService executor;

    @NonNull
    private final Duration timeout;

    /**
     * StreamLogNecromancer Utilities
     *
     * Necromancy is a supposed practice of magic involving communication with the deceased
     * – either by summoning their spirit as an apparition or raising them bodily. This suits
     * what this thread is tasked with, bringing back the SMR Maps from their grave (the Log).
     *
     */
    @Builder
    public StreamLogNecromancer(Duration timeout) {
        log.debug("Summon necromancer");

        this.timeout = timeout;

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("necromancer-%d")
                .build();

        executor = Executors.newSingleThreadExecutor(threadFactory);
    }

    public void invoke(ImmutableSortedMap<Long, ILogData> logDataMap, BiConsumer<Long, ILogData> resurrectionSpell) {
        log.debug("Invoke necromancer");

        CompletableFuture.runAsync(() -> logDataMap.forEach(resurrectionSpell), executor);
    }

    public void shutdown() {
        log.debug("Shutdown necromancer");

        executor.shutdown();

        try {
            executor.awaitTermination(timeout.toMinutes(), TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            String msg = "StreamLogNecromancer is taking too long to load the maps. Gave up.";
            throw new RecoveryException(msg);
        }
    }
}
