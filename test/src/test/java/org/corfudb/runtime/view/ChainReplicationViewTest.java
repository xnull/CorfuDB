package org.corfudb.runtime.view;

import java.time.Duration;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.LayoutProvider;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.corfudb.test.parameters.Server;
import org.corfudb.test.parameters.Servers;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.corfudb.test.parameters.Servers.SERVER_0;
import static org.corfudb.test.parameters.Servers.SERVER_1;
import static org.corfudb.test.parameters.Servers.SERVER_2;

/**
 * Created by mwei on 12/25/15.
 */
@CorfuTest
public class ChainReplicationViewTest {


    @CorfuTest
    public void canReadWriteToSingle(CorfuRuntime runtime)
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        runtime.getAddressSpaceView().write(new TokenResponse(0,
                        runtime.getLayoutView().getLayout().getEpoch(),
                        Collections.singletonMap(streamA, Address.NO_BACKPOINTER)),
                testPayload);

        assertThat(runtime.getAddressSpaceView().read(0L).getPayload(runtime))
                .isEqualTo("hello world".getBytes());

        assertThat(runtime.getAddressSpaceView().read(0L).containsStream(streamA))
                .isTrue();
    }

    @CorfuTest
    public void canReadWriteToSingleConcurrent(CorfuRuntime runtime,
                                            ConcurrentScheduler scheduler,
                                            @Parameter(Param.CONCURRENCY_SOME) int concurrency,
                                            @Parameter(Param.NUM_ITERATIONS_LARGE) int iterations,
                                            @Parameter(Param.TIMEOUT_LONG) Duration timeout)
            throws Exception {
        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                runtime.getAddressSpaceView().write(new TokenResponse((long)i,
                                runtime.getLayoutView().getLayout().getEpoch(),
                                Collections.singletonMap(CorfuRuntime.getStreamID("a"), Address.NO_BACKPOINTER)),
                        Integer.toString(i).getBytes());
            }
        });
        scheduler.execute(concurrency, timeout);

        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                assertThat(runtime.getAddressSpaceView().read(i).getPayload(runtime))
                        .isEqualTo(Integer.toString(i).getBytes());
            }
        });
        scheduler.execute(concurrency, timeout);
    }

    @LayoutProvider("3 Node Layout")
    final Layout layout3Node = Layout.builder()
                                        .layoutServerNode(SERVER_0.getLocator())
                                        .sequencerNode(SERVER_0.getLocator())
                                        .segment(LayoutSegment.builder()
                                            .replicationMode(ReplicationMode.CHAIN_REPLICATION)
                                            .stripe(LayoutStripe.builder()
                                                    .logServerNode(SERVER_0.getLocator())
                                                    .logServerNode(SERVER_1.getLocator())
                                                    .logServerNode(SERVER_2.getLocator())
                                                    .build())
                                            .build()
                                        )
                                    .build();

    @CorfuTest
    public void canReadWriteToMultiple(
        @Server(value = SERVER_0, initialLayout = "3 Node Layout")
        CorfuServer server0,
        @Server(value = SERVER_1, initialLayout = "3 Node Layout")
        CorfuServer server1,
        @Server(value = SERVER_2, initialLayout = "3 Node Layout")
        CorfuServer server2,
        CorfuRuntime runtime)
       throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        TokenResponse token = runtime.getSequencerView()
                                    .nextToken(Collections.singleton(streamA), 1);

        runtime.getAddressSpaceView().write(token, testPayload);

        assertThat(runtime.getAddressSpaceView().read(token.getTokenValue()).getPayload(runtime))
                .isEqualTo("hello world".getBytes());

        assertThat(runtime.getAddressSpaceView().read(token.getTokenValue())
                .containsStream(streamA)).isTrue();

        // Ensure that the data was written to each logunit.
        assertThat(server0.getServer(LogUnitServer.class))
            .matchesDataAtAddress(token.getTokenValue(), testPayload);
        assertThat(server1.getServer(LogUnitServer.class))
            .matchesDataAtAddress(token.getTokenValue(), testPayload);
        assertThat(server2.getServer(LogUnitServer.class))
            .matchesDataAtAddress(token.getTokenValue(), testPayload);
    }

}
