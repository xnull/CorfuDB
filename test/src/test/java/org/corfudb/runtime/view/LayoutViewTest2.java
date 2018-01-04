package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestRule;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.Layout.LayoutStripe;
import org.corfudb.runtime.view.Layout.ReplicationMode;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.LayoutProvider;
import org.corfudb.test.parameters.Servers;
import org.junit.Test;

@CorfuTest
public class LayoutViewTest2 {

    @LayoutProvider("single")
    Layout singleLayout = Layout.builder()
            .layoutServerNode(Servers.SERVER_0.getLocator())
            .sequencerNode(Servers.SERVER_0.getLocator())
            .segment(LayoutSegment.builder()
                        .replicationMode(ReplicationMode.CHAIN_REPLICATION)
                        .stripe(LayoutStripe.builder()
                                .logServerNode(Servers.SERVER_0.getLocator())
                                .build())
                        .build())
            .epoch(1)
        .build();

    @CorfuTest
    void canSetLayout(CorfuRuntime runtime)
        throws Exception {
        Layout l = new Layout(singleLayout);
        l.setRuntime(runtime);

        // Wait for the cluster ID to be available by fetching a layout.
        runtime.invalidateLayout();
        l.setClusterId(runtime.getClusterId());
        l.moveServersToEpoch();
        runtime.getLayoutView().updateLayout(l, 1L);
        runtime.invalidateLayout();
        assertThat(runtime.getLayoutView().getLayout().epoch)
            .isEqualTo(1L);
    }

    @CorfuTest
    void cannotSetLayoutWithWrongId(CorfuRuntime runtime)
        throws Exception {
        Layout l = new Layout(singleLayout);
        l.setRuntime(runtime);

        l.setRuntime(runtime);
        l.moveServersToEpoch();
        runtime.invalidateLayout();

        assertThatThrownBy(() -> runtime.getLayoutView().updateLayout(l, 1L))
            .isInstanceOf(WrongClusterException.class);
    }

}
