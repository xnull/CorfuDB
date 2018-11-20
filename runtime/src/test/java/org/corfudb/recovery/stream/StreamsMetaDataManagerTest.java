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


public class StreamsMetaDataManagerTest {

    @Test
    public void testGetStartAddress(){
        final long address = 1;
        final String stream1 = "stream1";
        final UUID streamId = CorfuRuntime.getStreamID(stream1);

        Map<CheckpointDictKey, String> mdKV = new HashMap<>();
        mdKV.put(CheckpointDictKey.START_TIME, "Start time");

        CheckpointEntry checkpoint = new CheckpointEntry(
                CheckpointEntryType.START,
                "test",
                UUID.randomUUID(),
                streamId,
                mdKV,
                null
        );

        LogData logData = new LogData(DataType.DATA, checkpoint);

        StreamsMetaDataManager manager = new StreamsMetaDataManager();
        manager.computeIfAbsent(streamId);
        manager.processCheckPointsInLogAddress(address, logData, checkpoint);

        assertThat(manager.streamsMetaData.size()).isEqualTo(100003);
    }

}