package org.corfudb.runtime.view.replication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.test.CorfuTest;
import org.corfudb.util.serializer.Serializers;

@CorfuTest
public interface IReplicationProtocolTest<T extends IReplicationProtocol> {

    T getProtocol();

    @CorfuTest
    default void canWriteRead(CorfuRuntime runtime)
        throws Exception {
        final T rp = getProtocol();
        final Layout layout = runtime.getLayoutView().getLayout();

        LogData data = getLogData(0, "hello world".getBytes());
        rp.write(layout, data);
        ILogData read = rp.read(layout, 0);

        assertThat(read.getType())
            .isEqualTo(DataType.DATA);
        assertThat(read.getGlobalAddress())
            .isEqualTo(0);
        assertThat(read.getPayload(runtime))
            .isEqualTo("hello world".getBytes());
    }


    /** Check to make sure reads never return empty in
     * the case of an unwritten address.
     */
    @CorfuTest
    default void readOnlyCommitted(CorfuRuntime runtime)
        throws Exception {
        final T rp = getProtocol();
        final Layout layout = runtime.getLayoutView().getLayout();

        ILogData read = rp.read(layout, 0);

        assertThat(read.getType())
            .isNotEqualTo(DataType.EMPTY);

        read = rp.read(layout, 1);

        assertThat(read.getType())
            .isNotEqualTo(DataType.EMPTY);
    }


    /** Check to make sure that overwriting a previously
     * written entry results in an OverwriteException.
     */
    @CorfuTest
    default void overwriteThrowsException(CorfuRuntime runtime)
        throws Exception {
        final T rp = getProtocol();
        final Layout layout = runtime.getLayoutView().getLayout();

        LogData d1 = getLogData(0, "1".getBytes());
        LogData d2 = getLogData(0, "2".getBytes());
        rp.write(layout, d1);
        assertThatThrownBy(() -> rp.write(layout, d2))
            .isInstanceOf(OverwriteException.class);
    }

    default LogData getLogData(long globalAddress, byte[] payload)
    {
        ByteBuf b = Unpooled.buffer();
        Serializers.CORFU.serialize(payload, b);
        LogData d = new LogData(DataType.DATA, b);
        d.setGlobalAddress(globalAddress);
        return d;
    }
}
