package org.corfudb.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

/**
 * Checkpoint multiple SMRMaps serially as a prerequisite for a later log trim.
 */
@Slf4j
public class MultiCheckpointWriter<T extends Map> {
    @Getter
    private List<ICorfuSMR<T>> maps = new ArrayList<>();

    @Getter
    private List<Long> checkpointLogAddresses = new ArrayList<>();

    @Setter
    @Getter
    boolean enablePutAll = false;

    /** Add a map to the list of maps to be checkpointed by this class. */
    @SuppressWarnings("unchecked")
    public void addMap(T map) {
        maps.add((ICorfuSMR<T>) map);
    }

    /** Add map(s) to the list of maps to be checkpointed by this class. */

    public void addAllMaps(Collection<T> maps) {
        for (T map : maps) {
            addMap(map);
        }
    }

    /** Checkpoint multiple SMRMaps serially.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @return Global log address of the first record of
     */
    public Token appendCheckpoints(CorfuRuntime rt, String author)
            throws Exception {
        return appendCheckpoints(rt, author, (x,y) -> { });
    }

    /** Checkpoint multiple SMRMaps serially.
     *
     * @param rt CorfuRuntime
     * @param author Author's name, stored in checkpoint metadata
     * @param postAppendFunc User-supplied lambda for post-append action on each
     *                       checkpoint entry type.
     * @return Global log address of the first record of
     */

    public Token appendCheckpoints(CorfuRuntime rt, String author,
                                  BiConsumer<CheckpointEntry, Long> postAppendFunc) {

        rt.getObjectsView().TXBuild()
                .type(TransactionType.SNAPSHOT)
                .build()
                .begin();

        Token snapshot = TransactionalContext.getCurrentContext().getSnapshotTimestamp();

        log.trace("appendCheckpoints: author '{}' at globalAddress {} begins",
                author, snapshot);

        log.info("appendCheckpoints: appending checkpoints for {} maps", maps.size());
        final long cpStart = System.currentTimeMillis();
        try {
            for (ICorfuSMR<T> map : maps) {
                UUID streamId = map.getCorfuStreamID();
                final long mapCpStart = System.currentTimeMillis();

                CheckpointWriter<T> cpw = new CheckpointWriter(rt, streamId, author, (T) map);
                cpw.setEnablePutAll(enablePutAll);
                ISerializer serializer =
                        ((CorfuCompileProxy<Map>) map.getCorfuSMRProxy())
                                .getSerializer();
                cpw.setSerializer(serializer);
                cpw.setPostAppendFunc(postAppendFunc);
                log.trace("appendCheckpoints: checkpoint map {} begin",
                        Utils.toReadableId(map.getCorfuStreamID()));
                List<Long> addresses = cpw.appendCheckpoint();
                log.trace("appendCheckpoints: checkpoint map {} end",
                        Utils.toReadableId(map.getCorfuStreamID()));
                checkpointLogAddresses.addAll(addresses);

                final long mapCpEnd = System.currentTimeMillis();

                log.info("appendCheckpoints: took {} ms to checkpoint map {}",
                        mapCpEnd - mapCpStart, streamId);
            }
        } finally {
            log.trace("appendCheckpoints: author '{}' at globalAddress {} finished",
                    author, snapshot);
            rt.getObjectsView().TXEnd();
        }
        final long cpStop = System.currentTimeMillis();

        log.info("appendCheckpoints: took {} ms to append {} checkpoints", cpStop - cpStart,
                maps.size());
        return snapshot;
    }

}
