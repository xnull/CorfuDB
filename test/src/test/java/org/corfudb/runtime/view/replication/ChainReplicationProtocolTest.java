package org.corfudb.runtime.view.replication;

import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.Servers;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test the chain replication protocol.
 *
 * Created by mwei on 4/11/17.
 */
@CorfuTest
public class ChainReplicationProtocolTest implements
                IReplicationProtocolTest<ChainReplicationProtocol> {

    @Override
    public ChainReplicationProtocol getProtocol() {
        return new ChainReplicationProtocol(new AlwaysHoleFillPolicy());
    }

    /** Check to see that a writer correctly
     * completes a failed write from another client.
     */
    @CorfuTest
    void failedWriteIsPropagated(CorfuRuntime runtime)
            throws Exception {
        final ChainReplicationProtocol rp = getProtocol();
        final Layout layout = runtime.getLayoutView().getLayout();

        LogData failedWrite = getLogData(0, "failed".getBytes());
        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        runtime.getRouter(Servers.SERVER_0.getLocator().toString())
                .getClient(LogUnitClient.class)
                .write(incompleteWrite).join();

        // Attempt to write using the replication protocol.
        // Should result in an overwrite exception
        assertThatThrownBy(() -> rp.write(layout, failedWrite))
                .isInstanceOf(OverwriteException.class);

        // At this point, a direct read of the tail should
        // reflect the -other- clients value
        ILogData readResult = runtime
                .getRouter(Servers.SERVER_0.getLocator().toString()).getClient(LogUnitClient.class)
                .read(0).get().getAddresses().get(0L);

        assertThat(readResult.getPayload(runtime))
            .isEqualTo("incomplete".getBytes());
    }


    /** Check to see that a read correctly
     * completes a failed write from another client.
     */
    @CorfuTest
    void failedWriteCanBeRead(CorfuRuntime runtime)
            throws Exception {
        final ChainReplicationProtocol rp = getProtocol();
        final Layout layout = runtime.getLayoutView().getLayout();

        LogData incompleteWrite = getLogData(0, "incomplete".getBytes());

        // Write the incomplete write to the head of the chain
        runtime.getRouter(Servers.SERVER_0.getLocator().toString()).getClient(LogUnitClient.class)
                .write(incompleteWrite).join();

        // At this point, a read
        // reflect the -other- clients value
        ILogData readResult = rp.read(layout, 0);

        assertThat(readResult.getPayload(runtime))
                .isEqualTo("incomplete".getBytes());
    }
}
