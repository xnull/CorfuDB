package org.corfudb.recovery.stream;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.exceptions.RecoveryException;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.corfudb.runtime.view.Address.isAddress;

@Slf4j
public class StreamTails {
    private final ConcurrentMap<UUID, Long> tails = new ConcurrentHashMap<>();

    // On checkpoint, we also need to track the stream tail of the checkpoint
    private void updateCheckpointStreamTail(ILogData logData) {
        // On checkpoint, we also need to track the stream tail of the checkpoint
        if (!logData.hasCheckpointMetadata()) {
            return;
        }

        if (logData.getCheckpointType() != CheckpointEntryType.END) {
            return;
        }

        if (!isAddress(logData.getCheckpointedStreamStartLogAddress())) {
            return;
        }

        UUID cpStreamId = logData.getCheckpointedStreamId();
        if (cpStreamId == null) {
            throw new IllegalStateException("Checkpointed stream id is null.");
        }

        Long oldAddress = tails.get(cpStreamId);
        if (oldAddress == null) {
            tails.put(cpStreamId, logData.getCheckpointedStreamStartLogAddress());
        } else {
            tails.put(cpStreamId, Math.max(oldAddress, logData.getCheckpointedStreamStartLogAddress()));
        }
    }

    /**
     * Book keeping of the the stream tails
     *
     * @param logData log data
     */
    public void updateStreamTails(long address, ILogData logData) {
        log.debug("Update stream tails for address: {}", address);

        updateCheckpointStreamTail(logData);

        for (UUID streamId : logData.getStreams()) {
            long streamAddress = Math.max(
                    address,
                    tails.getOrDefault(streamId, address)
            );

            tails.put(streamId, streamAddress);
        }
    }

    /**
     * Verifies whether there are any invalid streamTails.
     *
     */
    public void verifyStreamTails() {
        for (Long value : tails.values()) {
            if (value < 0) {
                log.error("Stream Tails map verification failed. Map = {}", tails);
                throw new RecoveryException("Invalid stream tails found in map.");
            }
        }
    }

    public ImmutableMap<UUID, Long> getTails() {
        return ImmutableMap.copyOf(tails);
    }

    public Long get(UUID streamId) {
        return tails.get(streamId);
    }

    public void clear() {
        tails.clear();
    }
}
