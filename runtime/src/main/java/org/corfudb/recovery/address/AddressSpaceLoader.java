package org.corfudb.recovery.address;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Range;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.recovery.address.AddressSpaceOffset;
import org.corfudb.runtime.view.AddressSpaceView;

import java.util.Map;

@Slf4j
public class AddressSpaceLoader {
    private static final int STATUS_UPDATE_PACE = 10000;

    @NonNull
    private final AddressSpaceView addressSpaceView;

    private final long batchReadSize;

    @Builder
    public AddressSpaceLoader(@NonNull AddressSpaceView addressSpaceView, long batchReadSize) {
        this.addressSpaceView = addressSpaceView;
        this.batchReadSize = batchReadSize;
    }

    /**
     * Fetch LogData from Corfu server
     *
     * @param address address to be fetched
     * @return LogData at address
     */
    public ILogData getLogData(boolean cacheDisabled, long address) {
        if (cacheDisabled) {
            return addressSpaceView.fetch(address);
        } else {
            return addressSpaceView.read(address);
        }
    }

    /**
     * Get a range of LogData from the server
     * <p>
     * This is using the underlying bulk read implementation for
     * fetching a range of addresses. This read will return
     * a map ordered by address.
     * <p>
     * It uses a ClosedOpen range : [start, end)
     * (e.g [0, 5) == (0,1,2,3,4))
     *
     * @return logData map ordered by addresses (increasing)
     */
    public ImmutableSortedMap<Long, ILogData> getLogData(long start, long end) {
        Map<Long, ILogData> result = addressSpaceView.cacheFetch(getAddressRange(start, end));
        return ImmutableSortedMap.copyOf(result);
    }

    @VisibleForTesting
    ContiguousSet<Long> getAddressRange(long start, long end) {
        return ContiguousSet.create(Range.closedOpen(start, end), DiscreteDomain.longs());
    }

    /**
     * Initialize log head and log tails
     * <p>
     * If logHead and logTail has not been initialized by
     * the user, initialize to default.
     */
    public AddressSpaceOffset buildAddressOffset() {
        return new AddressSpaceOffset(
                addressSpaceView.getTrimMark().getSequence(),
                addressSpaceView.getLogTail().getSequence(),
                batchReadSize
        );
    }

    /**
     * These two functions are called if no parameter were supplied
     * by the user.
     * Re ask for the Head, if it changes while we were trying
     */
    public long getTrimMark() {
        return addressSpaceView.getTrimMark().getSequence();
    }

    public void invalidateClientCache() {
        addressSpaceView.invalidateClientCache();
    }

    public boolean canProcessRange(int retryIteration, AddressSpaceOffset currOffset,
                                   ImmutableSortedMap<Long, ILogData> range) {

        for (Map.Entry<Long, ILogData> entry : range.entrySet()) {
            long address = entry.getKey();
            ILogData logData = entry.getValue();

            currOffset.incrementAddressProcessed();
            if (address != currOffset.getAddressProcessed()) {
                throw new IllegalStateException("We missed an entry. It can lead to correctness issues.");
            }

            if (logData.getType() == DataType.TRIMMED) {
                log.error("processLogData: FastObjectLoader retried to start {} times. Address is trimmed: {}",
                        retryIteration, currOffset.getLogHead());

                return false;
            }

            if (address % STATUS_UPDATE_PACE == 0) {
                log.info("applyForEachAddress: read up to {}", address);
            }
        }

        return true;
    }
}
