package org.corfudb.runtime.view;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.LayoutProvider;
import org.corfudb.test.parameters.Server;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.test.parameters.Servers.SERVER_0;
import static org.corfudb.test.parameters.Servers.SERVER_1;
import static org.corfudb.test.parameters.Servers.SERVER_2;

/**
 * Created by mwei on 2/1/16.
 */
@CorfuTest
public class AddressSpaceViewTest {

    @LayoutProvider("3 Stripe Layout")
    final Layout layout3NodeStriped = Layout.builder()
        .layoutServerNode(SERVER_0.getLocator())
        .sequencerNode(SERVER_0.getLocator())
        .segment(LayoutSegment.builder()
            .replicationMode(ReplicationMode.CHAIN_REPLICATION)
            .stripe(LayoutStripe.builder()
                .logServerNode(SERVER_0.getLocator())
                .build())
            .stripe(LayoutStripe.builder()
                .logServerNode(SERVER_1.getLocator())
                .build())
            .stripe(LayoutStripe.builder()
                .logServerNode(SERVER_2.getLocator())
                .build())
            .build()
        )
        .build();

    @CorfuTest
    public void ensureStripingWorks(
        @Server(value = SERVER_0, initialLayout = "3 Stripe Layout")
        CorfuServer server0,
        @Server(value = SERVER_1, initialLayout = "3 Stripe Layout")
        CorfuServer server1,
        @Server(value = SERVER_2, initialLayout = "3 Stripe Layout")
        CorfuServer server2,
        CorfuRuntime runtime)
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        TokenResponse token0 = runtime.getSequencerView()
                        .nextToken(Collections.singleton(streamA), 1);

        runtime.getAddressSpaceView().write(token0, "hello world".getBytes());

        assertThat(runtime.getAddressSpaceView().read(token0.getTokenValue()).getPayload(runtime))
                .isEqualTo("hello world".getBytes());

        assertThat(runtime.getAddressSpaceView().read(token0.getTokenValue()).containsStream(streamA))
                .isTrue();

        // Ensure that the data was written to the correct stripe
        List<CorfuServer> servers = ImmutableList.<CorfuServer>builder()
                                            .add(server0)
                                            .add(server1)
                                            .add(server2)
                                            .build();

        int write0Stripe = (int) token0.getTokenValue() % servers.size();
        LogUnitServerAssertions.assertThat(servers.get(write0Stripe).getServer(LogUnitServer.class))
                .matchesDataAtAddress(token0.getTokenValue(), testPayload);

        TokenResponse token1 = runtime.getSequencerView()
            .nextToken(Collections.singleton(streamA), 1);


        runtime.getAddressSpaceView().write(token1, "1".getBytes());

        // Ensure next data was written to the correct stripe.
        int write1Stripe = (int) token1.getTokenValue() %  servers.size();
        LogUnitServerAssertions.assertThat(servers.get(write1Stripe).getServer(LogUnitServer.class))
            .matchesDataAtAddress(token1.getTokenValue(), "1".getBytes());
    }

    @CorfuTest
    public void testGetTrimMark(CorfuRuntime runtime) {
        assertThat(runtime.getAddressSpaceView().getTrimMark()).isEqualTo(0);
        final long trimAddress = 10;

        runtime.getAddressSpaceView().prefixTrim(trimAddress);
        assertThat(runtime.getAddressSpaceView().getTrimMark()).isEqualTo(trimAddress + 1);
    }

    @CorfuTest
    public void ensureStripingReadAllWorks(
        @Server(value = SERVER_0, initialLayout = "3 Stripe Layout")
        CorfuServer server0,
        @Server(value = SERVER_1, initialLayout = "3 Stripe Layout")
            CorfuServer server1,
        @Server(value = SERVER_2, initialLayout = "3 Stripe Layout")
            CorfuServer server2,
        CorfuRuntime runtime)
            throws Exception {
        //configure the layout accordingly
        final TokenResponse token0 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);
        final TokenResponse token1 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);
        final TokenResponse token2 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);

        runtime.getAddressSpaceView().write(token0, "hello world".getBytes());
        runtime.getAddressSpaceView().write(token1, "1".getBytes());
        runtime.getAddressSpaceView().write(token2, "3".getBytes());

        List<Long> rs = Stream.of(token0, token1, token2)
            .map(TokenResponse::getTokenValue)
            .collect(Collectors.toList());

        Map<Long, ILogData> m = runtime.getAddressSpaceView().read(rs);

        assertThat(m.get(token0.getTokenValue()).getPayload(runtime))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(token1.getTokenValue()).getPayload(runtime))
                .isEqualTo("1".getBytes());
        assertThat(m.get(token2.getTokenValue()).getPayload(runtime))
                .isEqualTo("3".getBytes());
    }

    @CorfuTest
    public void readAllWithHoleFill(CorfuRuntime runtime)
            throws Exception {
        //configure the layout accordingly

        byte[] testPayload = "hello world".getBytes();

        final TokenResponse token0 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);
        final TokenResponse token1 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);
        final TokenResponse token2 = runtime.getSequencerView()
            .nextToken(Collections.emptySet(), 1);

        runtime.getAddressSpaceView().write(token0, testPayload);

        Range range = Range.closed(token0.getTokenValue(), token2.getTokenValue());
        ContiguousSet<Long> addresses = ContiguousSet.create(range, DiscreteDomain.longs());

        Map<Long, ILogData> m = runtime.getAddressSpaceView().read(addresses);

        assertThat(m.get(token0.getTokenValue()).getPayload(runtime))
                .isEqualTo("hello world".getBytes());
        assertThat(m.get(token1.getTokenValue()).isHole());
        assertThat(m.get(token2.getTokenValue()).isHole());
    }
}