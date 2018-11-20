package org.corfudb.recovery.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.recovery.stream.StreamMetaData.CheckPoint;
import org.corfudb.util.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointDictKey.SNAPSHOT_ADDRESS;

@Builder
@Slf4j
public class StreamsMetaDataManager {
    @VisibleForTesting
    final Map<UUID, StreamMetaData> streamsMetaData = new HashMap<>();

    /**
     * Find if there is a checkpoint in the current logAddress
     * <p>
     * If there is a checkpoint, the streamsMetadata map will be
     * updated accordingly.
     * <p>
     * We will only use the first checkpoint
     *
     * @param address address
     * @param logData log data
     */
    public void processCheckPointsInLogAddress(long address, ILogData logData, CheckpointEntry checkpoint) {
        log.debug("Find checkpoints in log address: {}", address);

        // Only one stream per checkpoint
        UUID streamId = logData.getCheckpointedStreamId();
        StreamMetaData streamMeta = computeIfAbsent(streamId);
        UUID checkPointId = logData.getCheckpointId();

        CheckpointEntryType checkpointType = logData.getCheckpointType();

        if (checkpointType == null) {
            String error = String.format("Null checkpoint. Stream: %s Address: %d", streamId, address);
            throw new IllegalStateException(error);
        }

        switch (checkpointType) {
            case START:
                handleStartCheckPoint(address, logData, checkPointId, streamMeta, checkpoint);
                break;
            case CONTINUATION:
                if (streamMeta.checkPointExists(checkPointId)) {
                    streamMeta.getCheckPoint(checkPointId).addAddress(address);
                }

                break;
            case END:
                if (streamMeta.checkPointExists(checkPointId)) {
                    streamMeta.getCheckPoint(checkPointId)
                            .setEnded(true)
                            .addAddress(address);
                    streamMeta.updateLatestCheckpointIfLater(checkPointId);
                }
                break;
            default:
                log.warn("findCheckPointsInLog[address = {}] Unknown checkpoint type", address);
                break;
        }
    }

    /**
     * When we encounter a start checkpoint, we need to create the new entry in the Stream
     *
     * @param address      entry address
     * @param logData      log data
     * @param checkPointId checkpoint id
     * @param streamMeta   meta information
     */
    private void handleStartCheckPoint(long address, ILogData logData, UUID checkPointId, StreamMetaData streamMeta,
                                       CheckpointEntry checkpoint) {
        log.debug("Handle startCheckpoint for address: {}, checkpoint id :{}", address, checkPointId);
        try {
            long snapshotAddress = getSnapShotAddressOfCheckPoint(checkpoint);
            long startAddress = logData.getCheckpointedStreamStartLogAddress();

            CheckPoint newCp = new CheckPoint(checkPointId)
                    .addAddress(address)
                    .setSnapshotAddress(snapshotAddress)
                    .setStartAddress(startAddress)
                    .setStarted(true);

            streamMeta.addCheckPoint(newCp);

        } catch (Exception e) {
            log.error("findCheckpointsInLogAddress[{}]: Couldn't get the snapshotAddress", address, e);
            throw new IllegalStateException("Couldn't get the snapshotAddress at address " + address);
        }
    }

    private long getSnapShotAddressOfCheckPoint(CheckpointEntry logEntry) {
        return Long.parseLong(logEntry.getDict().get(SNAPSHOT_ADDRESS));
    }

    /**
     * It is a valid state to have entries that are checkpointed but the log was not fully
     * trimmed yet. This means that some entries are at the same time in their own slot and in
     * the checkpoint. We must avoid to process them twice.
     * <p>
     * This comes especially relevant when the operation order affects the end result.
     * (e.g. clear() operation)
     *
     * @param streamId stream id to validate
     * @param entry    entry under scrutinization.
     * @return if the entry is already part of the checkpoint we started from.
     */
    public boolean entryAlreadyContainedInCheckpoint(UUID streamId, SMREntry entry) {
        if (!streamsMetaData.containsKey(streamId)) {
            return false;
        }

        return entry.getEntry().getGlobalAddress() < streamsMetaData.get(streamId).getHeadAddress();
    }

    public long getStartAddress(UUID streamId, UUID checkPointId) {
        return streamsMetaData
                .get(streamId)
                .getCheckPoint(checkPointId)
                .getStartAddress();

    }

    public StreamMetaData computeIfAbsent(UUID streamId) {
        return streamsMetaData.computeIfAbsent(streamId, StreamMetaData::new);
    }

    public ImmutableList<CheckPoint> getCheckPoints() {
        List<CheckPoint> checkpoints = streamsMetaData.values()
                .stream()
                .filter(metadata -> {
                    CheckPoint checkPoint = metadata.getLatestCheckPoint();
                    if (checkPoint == null) {
                        log.info(
                                "resurrectCheckpoints[{}]: Truncated checkpoint for this stream",
                                Utils.toReadableId(metadata.getStreamId())
                        );
                    }
                    return checkPoint != null;
                })
                .map(StreamMetaData::getLatestCheckPoint)
                .collect(Collectors.toList());

        return ImmutableList.copyOf(checkpoints);
    }
}
