package org.corfudb.recovery.stream;

import com.google.common.collect.ImmutableSet;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.recovery.stream.StreamLogLoader.StreamLogLoaderMode;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class StreamLogLoaderTest {

    @Test
    public void testLoad() {
        final String stream1 = "stream1";
        final String stream2 = "stream2";

        ImmutableSet<String> streams = ImmutableSet.of(stream1, stream2);

        ImmutableSet<UUID> expected = ImmutableSet.<UUID>builder()
                .add(CorfuRuntime.getStreamID(stream1))
                .add(CorfuRuntime.getCheckpointStreamIdFromName(stream1))
                .add(CorfuRuntime.getStreamID(stream2))
                .add(CorfuRuntime.getCheckpointStreamIdFromName(stream2))
                .build();


        StreamLogLoader loader = new StreamLogLoader(StreamLogLoaderMode.WHITELIST, streams);

        assertThat(loader.streams.size()).isEqualTo(4);
        assertThat(loader.streams.containsAll(expected)).isTrue();
    }

    @Test
    public void testWhiteStreamsMustBeProcessed() {
        final String stream1 = "stream1";
        final String stream2 = "stream2";

        ImmutableSet<String> streams = ImmutableSet.of(stream1, stream2);
        StreamLogLoader whitelistLoader = new StreamLogLoader(StreamLogLoaderMode.WHITELIST, streams);

        LogData logData = mock(LogData.class);
        when(logData.getStreams())
                .thenReturn(ImmutableSet.of(CorfuRuntime.getStreamID(stream2)));

        boolean shouldBeProcessed = whitelistLoader.shouldLogDataBeProcessed(logData);

        assertThat(shouldBeProcessed).isEqualTo(true);
    }

    @Test
    public void testStreamsNotInBlacklietMustdBeProcessed() {
        final String stream1 = "stream1";
        final String stream2 = "stream2";

        ImmutableSet<String> streams = ImmutableSet.of(stream1, stream2);
        StreamLogLoader whitelistLoader = new StreamLogLoader(StreamLogLoaderMode.WHITELIST, streams);

        LogData logData = mock(LogData.class);
        when(logData.getStreams())
                .thenReturn(ImmutableSet.of(CorfuRuntime.getStreamID("stream_not_in_blacklist")));

        boolean shouldBeProcessed = whitelistLoader.shouldLogDataBeProcessed(logData);

        assertThat(shouldBeProcessed).isEqualTo(true);
    }
}