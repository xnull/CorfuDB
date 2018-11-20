package org.corfudb.recovery.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class StreamLogLoader {

    @NonNull
    private final StreamLogLoaderMode mode;

    @NonNull
    @VisibleForTesting
    final ImmutableSet<UUID> streams;

    @Builder
    public StreamLogLoader(@NonNull StreamLogLoaderMode mode, @Singular ImmutableSet<String> streams) {
        this.mode = mode;
        this.streams = load(streams);
    }

    private ImmutableSet<UUID> load(ImmutableSet<String> streams) {
        log.debug("Load streams. Loader mode: {}", mode);

        Set<UUID> result = streams
                .stream()
                .flatMap(streamName -> {
                    UUID streamId = CorfuRuntime.getStreamID(streamName);
                    UUID checkpointId = CorfuRuntime.getCheckpointStreamIdFromId(streamId);
                    return Stream.of(streamId, checkpointId);
                })
                .collect(Collectors.toSet());

        return ImmutableSet.copyOf(result);
    }

    /**
     * If none of the streams in the logData should be processed, we
     * can simply ignore this logData.
     * <p>
     * In the case of a mix of streams we need to process and other that we don't,
     * we will still go ahead with the process.
     *
     * @param logData log data
     * @return flag - should be log data processed
     */
    public boolean shouldLogDataBeProcessed(ILogData logData) {
        boolean shouldProcess = false;
        for (UUID id : logData.getStreams()) {
            if (shouldStreamBeProcessed(id)) {
                shouldProcess = true;
                break;
            }
        }
        return shouldProcess;
    }

    public boolean shouldStreamBeProcessed(UUID streamId) {
        boolean result = false;
        switch (mode) {
            case WHITELIST:
                result = streams.contains(streamId);
                break;
            case BLACKLIST:
                result = !streams.contains(streamId);
                break;
        }

        return result;
    }

    public enum StreamLogLoaderMode {
        WHITELIST, BLACKLIST
    }
}
