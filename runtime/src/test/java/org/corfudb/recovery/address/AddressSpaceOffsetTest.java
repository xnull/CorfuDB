package org.corfudb.recovery.address;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AddressSpaceOffsetTest {
    private static final long LOG_HEAD = 1;
    private static final long LOG_TAIL = 100;

    @Test
    public void testDefaultOffset() {
        AddressSpaceOffset offset = new AddressSpaceOffset(LOG_HEAD, LOG_TAIL);
        assertThat(offset.getLogHead()).isEqualTo(LOG_HEAD);
        assertThat(offset.getLogTail()).isEqualTo(LOG_TAIL);
        assertThat(offset.getAddressProcessed()).isEqualTo(LOG_HEAD - 1);
        assertThat(offset.getNextRead()).isEqualTo(LOG_HEAD);
    }

    @Test
    public void testNextBatchReadOffset() {
        AddressSpaceOffset offset = new AddressSpaceOffset(LOG_HEAD, LOG_TAIL);
        offset = offset.nextBatchRead();

        assertThat(offset.getLogHead()).isEqualTo(LOG_HEAD);
        assertThat(offset.getLogTail()).isEqualTo(LOG_TAIL);
        assertThat(offset.getAddressProcessed()).isEqualTo(offset.getBatchReadSize() + 1);
        assertThat(offset.getNextRead()).isEqualTo(LOG_HEAD);
    }

    @Test
    public void testTrimOffset() {
        final long newLogHead = 35;
        AddressSpaceOffset offset = new AddressSpaceOffset(LOG_HEAD, LOG_TAIL);
        offset = offset.trim(newLogHead);

        assertThat(offset.getLogHead()).isEqualTo(newLogHead);
        assertThat(offset.getLogTail()).isEqualTo(LOG_TAIL);
        assertThat(offset.getAddressProcessed()).isEqualTo(newLogHead - 1);
        assertThat(offset.getNextRead()).isEqualTo(newLogHead);
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalTrimOffset() {
        final long trimMark = 0;
        AddressSpaceOffset offset = new AddressSpaceOffset(LOG_HEAD, LOG_TAIL);
        offset.trim(trimMark);
    }
}