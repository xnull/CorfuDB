package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import java.util.List;

import lombok.Data;

@Data
public class NettyTestFilter implements ICorfuPayload<NettyTestFilter> {

    private final List<NettyMsgTestFilter> messageFilters;

    public NettyTestFilter(List<NettyMsgTestFilter> messageFilters) {
        this.messageFilters = messageFilters;
    }

    public NettyTestFilter(ByteBuf buf) {
        messageFilters = ICorfuPayload.listFromBuffer(buf, NettyMsgTestFilter.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, messageFilters);
    }
}
