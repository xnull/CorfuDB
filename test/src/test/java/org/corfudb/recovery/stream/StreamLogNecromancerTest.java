package org.corfudb.recovery.stream;

import com.google.common.collect.ImmutableSortedMap;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.junit.Test;

import java.util.UUID;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

public class StreamLogNecromancerTest {

    @Test(timeout = 30 * 1000)
    public void testNecromancer() {
        StreamLogNecromancer necromancer = StreamLogNecromancer.builder().build();
        ImmutableSortedMap<Long, ILogData> log = ImmutableSortedMap
                .<Long, ILogData>naturalOrder()
                .build();
        final BiConsumer<Long, ILogData> streamLogProcessor = (address, logData) -> {
            long globalAddress = logData.getGlobalAddress();
            UUID streamId = logData.getStreams().iterator().next();

            //TODO check that we execute and apply changes from this (I mean exactly this) streamLogProcessor, check streamId, and address
            assertThat(1).isEqualTo(2);
        };

        necromancer.invoke(log, streamLogProcessor);
        necromancer.shutdown();
    }
}