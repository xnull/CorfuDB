package org.corfudb.recovery.stream;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointDictKey;
import org.corfudb.protocols.logprotocol.CheckpointEntry.CheckpointEntryType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamTailsTest {

    @Test
    public void updateStreamTails() {
        final long address = 5;
        final String streamName = "stream1";
        final UUID streamId = CorfuRuntime.getStreamID(streamName);

        StreamTails tails = new StreamTails();
        LogData logData = LogData.getTrimmed(address);

        tails.updateStreamTails(address, logData);
        assertThat(tails.getTails().isEmpty()).isTrue();

        Map<CheckpointDictKey, String> mdKV = new HashMap<>();
        mdKV.put(CheckpointDictKey.START_TIME, "Start time");

        CheckpointEntry cp = new CheckpointEntry(
                CheckpointEntryType.START,
                "test",
                UUID.randomUUID(),
                streamId,
                mdKV,
                null
        );

        logData = new LogData(DataType.DATA, cp);
        tails.updateStreamTails(address, logData);
        assertThat(tails.getTails().isEmpty()).isTrue();
    }
}