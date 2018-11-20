package org.corfudb.recovery;

import com.google.common.collect.ImmutableSortedMap;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.recovery.address.AddressSpaceLoader;
import org.corfudb.recovery.address.AddressSpaceOffset;
import org.corfudb.recovery.stream.CustomTypeStreams;
import org.corfudb.recovery.stream.StreamLogLoader;
import org.corfudb.recovery.stream.StreamLogNecromancer;
import org.corfudb.recovery.stream.StreamTails;
import org.corfudb.recovery.stream.StreamsMetaDataManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRProxy;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.ObjectsView.ObjectID;
import org.corfudb.util.ClassCastUtil;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;

@Slf4j
@Accessors(chain = true)
public abstract class FastLoader {
    @NonNull
    final AddressSpaceLoader addressSpaceLoader;

    @NonNull
    private final Duration timeout;

    public FastLoader(AddressSpaceLoader addressSpaceLoader, Duration timeout) {
        this.addressSpaceLoader = addressSpaceLoader;
        this.timeout = timeout;
    }

    /**
     * This method will apply for each address the consumer given in parameter.
     * <p>
     * The StreamLogNecromancer thread is used to do the heavy lifting.
     *
     * @param logDataProcessor log data processor
     */
    protected void applyForEachAddress(BiConsumer<Long, ILogData> logDataProcessor) {
        log.debug("Apply for each address");

        int retryIteration = 0;

        AddressSpaceOffset currOffset = addressSpaceLoader.buildAddressOffset();

        StreamLogNecromancer necromancer = StreamLogNecromancer.builder()
                .timeout(timeout)
                .build();

        try {
            while (currOffset.hasNext()) {
                long prevRead = currOffset.getNextRead();
                currOffset = currOffset.nextBatchRead();

                ImmutableSortedMap<Long, ILogData> range = loadData(currOffset, prevRead);

                boolean canProcessRange = addressSpaceLoader.canProcessRange(retryIteration, currOffset, range);

                if (canProcessRange) {
                    necromancer.invoke(range, logDataProcessor);
                } else {
                    retryIteration++;
                    currOffset = currOffset.trim(addressSpaceLoader.getTrimMark());
                    addressSpaceLoader.invalidateClientCache();
                    cleanUpForRetry();
                }
            }
        } finally {
            necromancer.shutdown();
        }
    }

    private ImmutableSortedMap<Long, ILogData> loadData(AddressSpaceOffset currOffset, long prevRead) {
        log.debug("Load data form: {}, to: {}", prevRead, currOffset.getNextRead());
        return addressSpaceLoader.getLogData(prevRead, currOffset.getNextRead());
    }

    protected abstract void cleanUpForRetry();

    /**
     * The FastObjectLoader reconstructs the coalesced state of SMRMaps through sequential log read
     * <p>
     * This utility reads Log entries sequentially extracting the SMRUpdates from each entry
     * and build the Maps as we go. In the presence of checkpoints, the checkpoint entries will
     * be applied before the normal entries starting after the checkpoint start address.
     * <p>
     * If used in the recoverSequencer mode, it will reconstruct the stream tails.
     * <p>
     * There are two main modes, blacklist and whitelist. These two modes are mutually exclusive:
     * In blacklist mode, we will process every streams as long as they are not in the streamToIgnore
     * list. In whitelist mode, only the streams present in streamsToLoad will be loaded. We make
     * sure to also include the checkpoint streams for each of them.
     * <p>
     * <p>
     * Created by rmichoud on 6/14/17.
     */
    @Slf4j
    public static class FastObjectLoader extends FastLoader {
        @NonNull
        private final ObjectsView objectsView;

        @NonNull
        private final FastObjectLoaderConfig config;

        @Getter
        @NonNull
        private final StreamLogLoader streamLoader;

        @NonNull
        @Getter
        private final StreamsMetaDataManager streamsMetaData;

        @NonNull
        @Getter
        private final CustomTypeStreams customTypeStreams;

        @Builder
        public FastObjectLoader(AddressSpaceLoader addressSpaceLoader, ObjectsView objectsView,
                                FastObjectLoaderConfig config, StreamLogLoader streamLoader,
                                StreamsMetaDataManager streamsMetaData, CustomTypeStreams customTypeStreams) {
            super(addressSpaceLoader, config.getTimeout());
            this.objectsView = objectsView;
            this.config = config;
            this.streamLoader = streamLoader;
            this.streamsMetaData = streamsMetaData;
            this.customTypeStreams = customTypeStreams;

        }

        /**
         * Entry point to load the SMRMaps in memory.
         * <p>
         * When this function returns, the maps are fully loaded.
         */
        public void load(CorfuRuntime runtime) {
            log.info("load: Starting to resurrect maps");
            recoverRuntime(runtime);

            log.info("loadMaps: Loading successful, Corfu maps are alive!");
        }

        /**
         * This method will use the checkpoints and the entries
         * after checkpoints to resurrect the SMRMaps
         */
        private void recoverRuntime(CorfuRuntime runtime) {
            log.info("recoverRuntime: Resurrecting the runtime");

            // If the user is sure that he has no checkpoint,
            // we can just do the last step. Risky, but the flag is
            // explicit enough.
            if (!config.isLogHasNoCheckPoint()) {
                applyForEachAddress((address, logData) -> {
                    Optional<LogEntry> entry = getLogEntry(runtime, logData);
                    entry.ifPresent(logEntry ->
                            findCheckPointsInLogAddress(address, logData, (CheckpointEntry) logEntry)
                    );
                });
                resurrectCheckpoints(runtime);
            }

            applyForEachAddress((address, logData) -> {
                getLogEntry(runtime, logData).ifPresent(logEntry -> processLogData(logData, logEntry));
            });
        }

        private Optional<LogEntry> getLogEntry(CorfuRuntime runtime, ILogData logData) {
            try {
                return Optional.of(logData.getLogEntry(runtime));
            } catch (Exception e) {
                log.error("Cannot deserialize log entry" + logData.getGlobalAddress(), e);
            }

            return Optional.empty();
        }

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
        private void findCheckPointsInLogAddress(long address, ILogData logData, CheckpointEntry checkpoint) {
            if (!logData.hasCheckpointMetadata() || !streamLoader.shouldLogDataBeProcessed(logData)) {
                return;
            }

            streamsMetaData.processCheckPointsInLogAddress(address, logData, checkpoint);
        }

        /**
         * Apply the checkPoints in parallel
         * <p>
         * Since each checkpoint is mapped to a single stream, we can parallelize
         * this operation.
         *
         * @param runtime
         */
        private void resurrectCheckpoints(CorfuRuntime runtime) {
            streamsMetaData
                    .getCheckPoints()
                    .parallelStream()
                    .flatMap(cp -> cp.getAddresses().stream())
                    .forEach(address -> {
                        ILogData logData = addressSpaceLoader.getLogData(config.isCacheDisabled(), address);
                        getLogEntry(runtime, logData).ifPresent(logEntry -> updateCorfuObject(logData, logEntry));
                    });
        }

        /**
         * Extract log entries from logData and update the Corfu Objects
         *
         * @param logData LogData received from Corfu server.
         */
        private void updateCorfuObject(ILogData logData, LogEntry logEntry) {
            long globalAddress = logData.getGlobalAddress();

            switch (logEntry.getType()) {
                case SMR:
                    updateCorfuObjectWithSmrEntry(logData, logEntry, globalAddress);
                    break;
                case MULTIOBJSMR:
                    updateCorfuObjectWithMultiObjSmrEntry(logEntry, globalAddress);
                    break;
                case CHECKPOINT:
                    updateCorfuObjectWithCheckPointEntry(logEntry);
                    break;
                default:
                    log.warn("updateCorfuObject[address = {}]: Unknown data type");

            }
        }

        private void updateCorfuObjectWithSmrEntry(ILogData logData, LogEntry logEntry, long globalAddress) {
            UUID streamId = logData.getStreams().iterator().next();
            applySmrEntryToStream(streamId, (SMREntry) logEntry, globalAddress);
        }

        private void updateCorfuObjectWithMultiObjSmrEntry(LogEntry logEntry, long globalAddress) {
            MultiObjectSMREntry multiObjectLogEntry = (MultiObjectSMREntry) logEntry;
            multiObjectLogEntry.getEntryMap().forEach((streamId, multiSmrEntry) -> {
                multiSmrEntry.getSMRUpdates(streamId).forEach((smrEntry) -> {
                    applySmrEntryToStream(streamId, smrEntry, globalAddress);
                });
            });
        }

        private void applySmrEntryToStream(UUID streamId, SMREntry entry, long globalAddress) {
            applySmrEntryToStream(streamId, entry, globalAddress, false);
        }

        private void updateCorfuObjectWithCheckPointEntry(LogEntry logEntry) {
            CheckpointEntry checkPointEntry = (CheckpointEntry) logEntry;
            // Just one stream, always
            UUID streamId = checkPointEntry.getStreamId();
            UUID checkPointId = checkPointEntry.getCheckpointId();

            // We need to apply the start address for the version of the object
            long startAddress = streamsMetaData.getStartAddress(streamId, checkPointId);

            // We don't know in advance if there will be smrEntries
            if (checkPointEntry.getSmrEntries() == null) {
                return;
            }

            checkPointEntry.getSmrEntries()
                    .getSMRUpdates(streamId)
                    .forEach(smrEntry -> {
                        applySmrEntryToStream(checkPointEntry.getStreamId(), smrEntry, startAddress, true);
                    });
        }

        /**
         * Check if this entry is relevant
         * <p>
         * There are 3 cases where an entry should not be processed:
         * 1. In whitelist mode, if the stream is not in the whitelist (streamToLoad)
         * 2. In blacklist mode, if the stream is in the blacklist (streamToIgnore)
         * 3. If the entry was already processed in the previous checkpoint.
         *
         * @param streamId identifies the Corfu stream
         * @param entry    entry to potentially apply
         * @return if we need to apply the entry.
         */
        private boolean shouldEntryBeApplied(UUID streamId, SMREntry entry, boolean isCheckpointEntry) {
            if (!streamLoader.shouldStreamBeProcessed(streamId)) {
                return false;
            }

            // We ignore the transaction stream ID because it is a raw stream
            // We don't want to create a Map for it.
            if (ObjectsView.TRANSACTION_STREAM_ID.equals(streamId)) {
                return false;
            }

            if (isCheckpointEntry) {
                return true;
            }

            // 3.
            // If the entry was already processed with the previous checkpoint.
            return !streamsMetaData.entryAlreadyContainedInCheckpoint(streamId, entry);
        }

        /**
         * Update the corfu object and it's underlying stream with the new entry.
         *
         * @param streamId          identifies the Corfu stream
         * @param entry             entry to apply
         * @param globalAddress     global address of the entry
         * @param isCheckPointEntry is entry a checkpoint
         */
        private void applySmrEntryToStream(UUID streamId, SMREntry entry, long globalAddress, boolean isCheckPointEntry) {
            if (!shouldEntryBeApplied(streamId, entry, isCheckPointEntry)) {
                return;
            }

            // Get the serializer type from the entry
            ISerializer serializer = Serializers.getSerializer(entry.getSerializerType().getType());

            // Get the type of the object we want to recreate
            Class<?> objectType = customTypeStreams.getStreamType(streamId);

            // Create an Object only for non-checkpoints

            // If it is a special type, create it with the object builder
            ObjectID<?> objId = ObjectBuilder.buildObjectId(streamId, objectType);
            if (!objectsView.contains(objId)) {
                ObjectBuilder<?> objectBuilder = customTypeStreams
                        .get(streamId)
                        .orElseGet(() -> objectsView.build().setStreamID(streamId).setType(objectType));

                objectBuilder.setSerializer(serializer).open();
            }

            ICorfuSMR<?> smrObject = (ICorfuSMR<?>) objectsView.get(objId);
            ICorfuSMRProxy<?> object = smrObject.getCorfuSMRProxy();

            Optional<CorfuCompileProxy<?>> objectProxy = ClassCastUtil.cast(object);

            if (objectProxy.isPresent()) {
                objectProxy.get()
                        .getUnderlyingObject()
                        .applyUpdateToStreamUnsafe(entry, globalAddress);
            } else {
                String error = String.format(
                        "Can't apply Smr entry to stream: %s, wrong object type: %s",
                        objectType, streamId
                );
                throw new IllegalStateException(error);
            }
        }

        /**
         * Dispatch logData given it's type
         *
         * @param logData log data
         */
        private void processLogData(ILogData logData, LogEntry logEntry) {

            switch (logData.getType()) {
                case DATA:
                    // Checkpoint should have been processed first
                    if (logData.hasCheckpointMetadata() || !streamLoader.shouldLogDataBeProcessed(logData)) {
                        return;
                    }

                    updateCorfuObject(logData, logEntry);
                    break;
                case HOLE:
                    log.debug("HOLE log data will not be processed. Streams: " + logData.getStreams());
                    break;
                case TRIMMED:
                    log.debug("TRIMMED log data will not be processed. Streams: " + logData.getStreams());
                    break;
                case EMPTY:
                    log.warn("applyForEachAddress[address={}] is empty");
                    break;
                case RANK_ONLY:
                    log.debug("RANK_ONLY log data will not be processed. Streams: " + logData.getStreams());
                    break;
                default:
                    break;
            }
        }

        public void addIndexerToCorfuTableStream(String streamName, CorfuTable.IndexRegistry<?, ?> indexRegistry,
                                                 CorfuRuntime runtime) {
            customTypeStreams.addIndexerToCorfuTableStream(streamName, indexRegistry, runtime);
        }

        @Override
        protected void cleanUpForRetry() {
            objectsView.clear();
        }
    }

    @Slf4j
    public static class FastSequencerLoader extends FastLoader {

        @NonNull
        @Getter
        private final StreamTails streamTails = new StreamTails();

        @Builder
        public FastSequencerLoader(AddressSpaceLoader addressSpaceLoader, Duration timeout) {
            super(addressSpaceLoader, timeout);
        }

        public FastSequencerLoader load() {
            recoverSequencer();
            return this;
        }

        /**
         * This method will only resurrect the stream tails. It is used
         * to recover a sequencer.
         */
        private void recoverSequencer() {
            log.info("recoverSequencer: Resurrecting the stream tails");
            applyForEachAddress(streamTails::updateStreamTails);
        }

        @Override
        protected void cleanUpForRetry() {
            streamTails.clear();
        }
    }
}
