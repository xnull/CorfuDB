package org.corfudb.recovery.address;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.ImmutableSortedMap;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.recovery.address.AddressSpaceLoader;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.Test;

import java.util.SortedSet;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AddressSpaceLoaderTest {
    final long batchReadSize = 10;

    @Test
    public void testGetLogData(){
        final long address = 1;

        ILogData logDataMock = mock(ILogData.class);
        AddressSpaceView viewMock = mock(AddressSpaceView.class);
        when(viewMock.fetch(address)).thenReturn(logDataMock);
        when(viewMock.read(address)).thenReturn(logDataMock);

        AddressSpaceLoader loader = new AddressSpaceLoader(viewMock, batchReadSize);
        loader.getLogData(true, address);
        verify(viewMock).fetch(address);

        loader.getLogData(false, address);
        verify(viewMock).read(address);
    }

    @Test
    public void testGetAddressRange(){
        final long start = 1;
        final long end = 3;

        AddressSpaceView viewMock = mock(AddressSpaceView.class);

        AddressSpaceLoader loader = new AddressSpaceLoader(viewMock, batchReadSize);
        ContiguousSet<Long> range = loader.getAddressRange(start, end);

        SortedSet<Long> expectedRange = new TreeSet<>();
        for (long i = start; i < end; i++) {
            expectedRange.add(i);
        }

        assertThat(range.containsAll(expectedRange)).isTrue();
    }

    @Test
    public void canProcessRange() {
        final long start = 1;
        final long end = 3;
        final int retryIteration = 1;

        AddressSpaceView viewMock = mock(AddressSpaceView.class);
        AddressSpaceLoader loader = new AddressSpaceLoader(viewMock, batchReadSize);

        final ILogData logData1 = mock(ILogData.class);
        when(logData1.getType()).thenReturn(DataType.TRIMMED);

        ImmutableSortedMap<Long, ILogData> range = ImmutableSortedMap.<Long, ILogData>naturalOrder()
                .put(1L, logData1)
                .build();

        AddressSpaceOffset offset = new AddressSpaceOffset(start, end);
        boolean canBeProcessed = loader.canProcessRange(retryIteration, offset, range);

        assertThat(offset.getAddressProcessed()).isEqualTo(1);
        assertThat(canBeProcessed).isFalse();
    }
}