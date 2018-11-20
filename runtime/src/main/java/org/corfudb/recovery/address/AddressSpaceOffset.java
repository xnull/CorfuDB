package org.corfudb.recovery.address;

import lombok.Getter;

import java.util.StringJoiner;

public class AddressSpaceOffset {
    private static final long DEFAULT_BATCH_FOR_FAST_LOADER = 10;
    @Getter
    private final long batchReadSize;

    @Getter
    private final long logHead;
    @Getter
    private final long logTail;
    @Getter
    private long addressProcessed;

    @Getter
    private final long nextRead;

    public AddressSpaceOffset(long logHead, long logTail) {
        this(logHead, logTail, logHead - 1, DEFAULT_BATCH_FOR_FAST_LOADER);
    }

    public AddressSpaceOffset(long logHead, long logTail, long batchReadSize) {
        this(logHead, logTail, logHead - 1, batchReadSize);
    }

    private AddressSpaceOffset(long logHead, long logTail, long addressProcessed, long batchReadSize) {
        this.logHead = logHead;
        this.logTail = logTail;
        this.nextRead = logHead;
        this.addressProcessed = addressProcessed;
        this.batchReadSize = batchReadSize;
    }

    public boolean hasNext() {
        return nextRead <= logTail;
    }

    public AddressSpaceOffset trim(long newLogHead) {
        if (newLogHead < logHead){
            throw new IllegalStateException("Invalid trim. Old logHead: " + logHead + ", newLogHead: " + newLogHead);
        }

        return new AddressSpaceOffset(newLogHead, logTail, newLogHead -1, batchReadSize);
    }

    public AddressSpaceOffset nextBatchRead() {
        return new AddressSpaceOffset(
                logHead,
                logTail,
                Math.min(nextRead + batchReadSize, logTail + 1),
                batchReadSize
        );
    }

    public void incrementAddressProcessed(){
        addressProcessed++;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", AddressSpaceOffset.class.getSimpleName() + "[", "]")
                .add("batchReadSize=" + batchReadSize)
                .add("logHead=" + logHead)
                .add("logTail=" + logTail)
                .add("addressProcessed=" + addressProcessed)
                .toString();
    }
}
