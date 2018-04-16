package org.corfudb.infrastructure;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.NettyMsgTestFilter;
import org.corfudb.protocols.wireprotocol.NettyTestFilter;

@Slf4j
public class TestInboundHandler extends ChannelDuplexHandler {

    Map<CorfuMsgType, NettyMsgTestFilter> msgTestFilterMap = new ConcurrentHashMap<>();

    TestInboundHandler() {
        log.info("TestInboundHandler initialized.");
    }

    private void modifyFilter(NettyTestFilter filter) {
        filter.getMessageFilters().forEach(nettyMsgTestFilter ->
                msgTestFilterMap.put(nettyMsgTestFilter.getCorfuMsgType(), nettyMsgTestFilter));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        CorfuMsg corfuMsg = (CorfuMsg) msg;
        if (corfuMsg.getMsgType().equals(CorfuMsgType.TEST_FILTER_MSG)) {
            modifyFilter(((CorfuPayloadMsg<NettyTestFilter>) corfuMsg).getPayload());
        }

        super.channelRead(ctx, msg);
    }
}
