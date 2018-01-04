package org.corfudb.runtime.view.replication;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.LogUnitServerAssertions;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.infrastructure.TestServerRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.DataOutrankedException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.CorfuTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test the quorum replication protocol.
 *
 * Created by mwei on 4/11/17.
 */
@CorfuTest
public class QuorumReplicationProtocolTest
    implements IReplicationProtocolTest<QuorumReplicationProtocol> {


    public static final UUID testClientId = UUID.nameUUIDFromBytes("TEST_CLIENT".getBytes());

    @Override
    public QuorumReplicationProtocol getProtocol() {
        return new QuorumReplicationProtocol(new  AlwaysHoleFillPolicy());
    }

    @Override
    @CorfuTest
    public void overwriteThrowsException(CorfuRuntime runtime)
            throws Exception {
        // currently we don't give full waranties if two clients race for
        // the same position during the 1-phase quorum write.
        // this is just assertion for wrong code, but in general this
        // operation could do harm
        IReplicationProtocolTest.super.overwriteThrowsException(runtime);
    }


}
