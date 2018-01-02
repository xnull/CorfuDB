package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.orchestrator.CreateWorkflowResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTable.NoSecondaryIndex;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.junit.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 * This integration test verifies the behaviour of the add node workflow. In particular, a single node
 * cluster is created and then populated with data, then a new node is added to the cluster,
 * making it of size 2. Checkpointing is then triggered so that the new second node starts servicing
 * new writes, then a 3rd node is added. The third node will have the checkpoints of the CorfuTable
 * that were populated and checkpointed when the cluster was only 2 nodes. Finally, a client reads
 * back the data generated while growing the cluster to verify that it is correct and can be read
 * from a three node cluster.
 *
 * Created by Maithem on 12/1/17.
 */
@Slf4j
public class WorkflowIT extends AbstractIT {

    final String host = "localhost";

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    @Test
    public void AddNodeIT() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int n1Port = 9000;

        // Start node one and populate it with data
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .setSingle(true)
                .runServer();

        CorfuRuntime n1Rt = CorfuRuntime.fromParameters(
            CorfuRuntimeParameters.builder()
                .layoutServer(
                    NodeLocator.builder()
                        .host(host)
                        .port(n1Port)
                        .build())
                .build());

        n1Rt.connect();

        CorfuTable<String, String, NoSecondaryIndex, Void> table =
            n1Rt.getObjectsView()
                .build()
                .setTypeToken(
                    new TypeToken<CorfuTable<String, String, NoSecondaryIndex, Void>>() {})
                .setStreamName(streamName)
                .open();

        for (int x = 0; x < PARAMETERS.NUM_ITERATIONS_MODERATE; x++) {
            table.put(String.valueOf(x), String.valueOf(x));
        }

        // Add a second node
        final int n2Port = 9001;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .runServer();

        // Since server started in single node, sequencer is same node as management server
        final String mgmtNode = n1Rt.getLayoutView().getLayout().getSequencers().get(0);
        final NodeLocator mgmtLocator = NodeLocator.parseString(mgmtNode);

        CreateWorkflowResponse resp = getManagementClient(n1Rt, mgmtLocator)
                                            .addNodeRequest(getConnectionString(n2Port));

        assertThat(resp.getWorkflowId()).isNotNull();

        waitForWorkflow(resp.getWorkflowId(), n1Rt, mgmtLocator);

        n1Rt.invalidateLayout();
        final int clusterSizeN2 = 2;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN2);

        // Verify that the workflow ID for node 2 is no longer active
        assertThat(getManagementClient(n1Rt, mgmtLocator)
                        .queryRequest(resp.getWorkflowId()).isActive()).isFalse();

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(table);

        long prefix = mcw.appendCheckpoints(n1Rt, "Maithem");

        n1Rt.getAddressSpaceView().prefixTrim(prefix - 1);

        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();
        n1Rt.getAddressSpaceView().gc();

        // Add a third node after compaction

        final int n3Port = 9002;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n3Port)
                .runServer();

        CreateWorkflowResponse resp2 = getManagementClient(n1Rt, mgmtLocator)
                                            .addNodeRequest(getConnectionString(n3Port));

        assertThat(resp2.getWorkflowId()).isNotNull();

        waitForWorkflow(resp2.getWorkflowId(), n1Rt, mgmtLocator);

        // Verify that the third node has been added and data can be read back
        n1Rt.invalidateLayout();

        final int clusterSizeN3 = 3;
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(clusterSizeN3);
        // Verify that the workflow ID for node 3 is no longer active
        assertThat(getManagementClient(n1Rt, mgmtLocator)
                            .queryRequest(resp2.getWorkflowId()).isActive()).isFalse();

        for (int x = 0; x < PARAMETERS.NUM_ITERATIONS_MODERATE; x++) {
            String v = table.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
        }
    }

    /** Get a management client using a {@link CorfuRuntime} and {@link NodeLocator},
     *  setting it's epoch to the latest epoch.
     * @param runtime   The {@link CorfuRuntime} to use
     * @param locator   The {@link NodeLocator} to use
     * @return  A {@link ManagementClient}
     */
    private ManagementClient getManagementClient(@Nonnull CorfuRuntime runtime,
                                         @Nonnull NodeLocator locator) {
        IClientRouter router = runtime.getRouter(locator.toString());
        // Make sure router is in the correct epoch.
        router.setEpoch(runtime.getLayoutView().getLayout().getEpoch());
        return router.getClient(ManagementClient.class);
    }

    void waitForWorkflow(@Nonnull UUID id,
                         @Nonnull CorfuRuntime rt,
                         @Nonnull NodeLocator locator) {
        for (int x = 0; x < PARAMETERS.NUM_ITERATIONS_LOW; x++) {
            try {
                ManagementClient mgmt = getManagementClient(rt, locator);
                if (mgmt.queryRequest(id).isActive()) {
                    Sleep.sleepUninterruptibly(PARAMETERS.TIMEOUT_SHORT);
                } else {
                    return;
                }
            } catch (WrongEpochException e) {
                rt.invalidateLayout();
            }
        }
        assertThat(false)
            .as("Workflow is still active after " + PARAMETERS.NUM_ITERATIONS_LOW
                + " retries")
            .isEqualTo(true);
    }
}
