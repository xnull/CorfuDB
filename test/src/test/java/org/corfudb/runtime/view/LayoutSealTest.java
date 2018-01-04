package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.test.assertions.CorfuAssertions.assertThat;
import static org.corfudb.test.util.ServerTestUtil.disconnectAndBlacklistClientFromServer;

import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.LayoutProvider;
import org.corfudb.test.parameters.Server;
import org.corfudb.test.parameters.Servers;

@CorfuTest
public class LayoutSealTest {

    @LayoutProvider("5server-chain")
    Layout layout5ServerChain = Layout.builder()
        .layoutServerNode(Servers.SERVER_0.getLocator())
        .layoutServerNode(Servers.SERVER_1.getLocator())
        .layoutServerNode(Servers.SERVER_2.getLocator())
        .sequencerNode(Servers.SERVER_0.getLocator())
        .segment(LayoutSegment.builder()
            .replicationMode(ReplicationMode.CHAIN_REPLICATION)
            .stripe(LayoutStripe.builder()
                .logServerNode(Servers.SERVER_0.getLocator())
                .logServerNode(Servers.SERVER_1.getLocator())
                .logServerNode(Servers.SERVER_2.getLocator())
                .build())
            .stripe(LayoutStripe.builder()
                .logServerNode(Servers.SERVER_3.getLocator())
                .logServerNode(Servers.SERVER_4.getLocator())
                .build())
            .build())
        .build();


    @LayoutProvider("5server-quorum")
    Layout layout5ServerQuorum = Layout.builder()
        .layoutServerNode(Servers.SERVER_0.getLocator())
        .layoutServerNode(Servers.SERVER_1.getLocator())
        .layoutServerNode(Servers.SERVER_2.getLocator())
        .sequencerNode(Servers.SERVER_0.getLocator())
        .segment(LayoutSegment.builder()
            .replicationMode(ReplicationMode.QUORUM_REPLICATION)
            .stripe(LayoutStripe.builder()
                .logServerNode(Servers.SERVER_0.getLocator())
                .logServerNode(Servers.SERVER_1.getLocator())
                .logServerNode(Servers.SERVER_2.getLocator())
                .build())
            .stripe(LayoutStripe.builder()
                .logServerNode(Servers.SERVER_3.getLocator())
                .logServerNode(Servers.SERVER_4.getLocator())
                .build())
            .build())
        .build();

    /**
     * Scenario: 5 Servers.
     * All working normally and attempted to seal.
     * Seal passes.
     */
    @CorfuTest
    public void successfulChainSeal(
        @Server(value = Servers.SERVER_0, initialLayout = "5server-chain") CorfuServer server0,
        @Server(value = Servers.SERVER_1, initialLayout = "5server-chain") CorfuServer server1,
        @Server(value = Servers.SERVER_2, initialLayout = "5server-chain") CorfuServer server2,
        @Server(value = Servers.SERVER_3, initialLayout = "5server-chain") CorfuServer server3,
        @Server(value = Servers.SERVER_4, initialLayout = "5server-chain") CorfuServer server4,
        CorfuRuntime runtime) {
        Layout newLayout = new Layout(layout5ServerChain);
        newLayout.setEpoch(2);
        newLayout.setRuntime(runtime);

        assertThatCode(newLayout::moveServersToEpoch)
            .doesNotThrowAnyException();;

        assertThat(server0).hasEpoch(2);
        assertThat(server1).hasEpoch(2);
        assertThat(server2).hasEpoch(2);
        assertThat(server3).hasEpoch(2);
        assertThat(server4).hasEpoch(2);
    }

    /**
     * Scenario: 5 Servers.
     * ENDPOINT_1, ENDPOINT_3 and ENDPOINT_3 failed and attempted to seal.
     * LayoutServers quorum is possible,    -   Seal passes
     * Stripe 1: 1 failed, 2 responses.     -   Seal passes
     * Stripe 2: 2 failed, 0 responses.     -   Seal failed
     * Seal failed
     */
    @CorfuTest
    void failingChainSeal(
        @Server(value = Servers.SERVER_0, initialLayout = "5server-chain") CorfuServer server0,
        @Server(value = Servers.SERVER_1, initialLayout = "5server-chain") CorfuServer server1,
        @Server(value = Servers.SERVER_2, initialLayout = "5server-chain") CorfuServer server2,
        @Server(value = Servers.SERVER_3, initialLayout = "5server-chain") CorfuServer server3,
        @Server(value = Servers.SERVER_4, initialLayout = "5server-chain") CorfuServer server4,
        CorfuRuntime runtime) {
        Layout newLayout = new Layout(layout5ServerChain);
        newLayout.setEpoch(2);
        newLayout.setRuntime(runtime);

        disconnectAndBlacklistClientFromServer(runtime, server1);
        disconnectAndBlacklistClientFromServer(runtime, server3);
        disconnectAndBlacklistClientFromServer(runtime, server4);

        assertThatThrownBy(newLayout::moveServersToEpoch)
            .isInstanceOf(QuorumUnreachableException.class);

        assertThat(server0).hasEpoch(2);
        assertThat(server1).hasEpoch(1);
        assertThat(server2).hasEpoch(2);
        assertThat(server3).hasEpoch(1);
        assertThat(server4).hasEpoch(1);
    }


    /**
     * Scenario: 5 Servers.
     * All working normally and attempted to seal.
     * Seal passes.
     */
    @CorfuTest
    void successfulQuorumSeal(
        @Server(value = Servers.SERVER_0, initialLayout = "5server-quorum") CorfuServer server0,
        @Server(value = Servers.SERVER_1, initialLayout = "5server-quorum") CorfuServer server1,
        @Server(value = Servers.SERVER_2, initialLayout = "5server-quorum") CorfuServer server2,
        @Server(value = Servers.SERVER_3, initialLayout = "5server-quorum") CorfuServer server3,
        @Server(value = Servers.SERVER_4, initialLayout = "5server-quorum") CorfuServer server4,
        CorfuRuntime runtime) {
        Layout l = new Layout(layout5ServerQuorum);
        l.setRuntime(runtime);
        l.setEpoch(2);

        assertThatCode(l::moveServersToEpoch)
            .doesNotThrowAnyException();

        assertThat(server0).hasEpoch(2);
        assertThat(server1).hasEpoch(2);
        assertThat(server2).hasEpoch(2);
        assertThat(server3).hasEpoch(2);
        assertThat(server4).hasEpoch(2);
    }

    /**
     * Scenario: 5 Servers.
     * ENDPOINT_3 failed and attempted to seal.
     * LayoutServers quorum is possible,    -   Seal passes
     * Stripe 1: 0 failed, 3 responses.     -   Seal passes
     * Stripe 2: 1 failed, 1 response.      -   Seal failed (Quorum not possible)
     * Seal failed
     */
    @CorfuTest
    void failingQuorumSeal(
        @Server(value = Servers.SERVER_0, initialLayout = "5server-quorum") CorfuServer server0,
        @Server(value = Servers.SERVER_1, initialLayout = "5server-quorum") CorfuServer server1,
        @Server(value = Servers.SERVER_2, initialLayout = "5server-quorum") CorfuServer server2,
        @Server(value = Servers.SERVER_3, initialLayout = "5server-quorum") CorfuServer server3,
        @Server(value = Servers.SERVER_4, initialLayout = "5server-quorum") CorfuServer server4,
        CorfuRuntime runtime) {
        Layout newLayout = new Layout(layout5ServerQuorum);
        newLayout.setEpoch(2);
        newLayout.setRuntime(runtime);

        disconnectAndBlacklistClientFromServer(runtime, server3);

        assertThatThrownBy(newLayout::moveServersToEpoch)
            .isInstanceOf(QuorumUnreachableException.class);

        assertThat(server0).hasEpoch(2);
        assertThat(server1).hasEpoch(2);
        assertThat(server2).hasEpoch(2);
        assertThat(server3).hasEpoch(1);
        assertThat(server4).hasEpoch(2);
    }
}
