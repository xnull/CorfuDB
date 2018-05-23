package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;
import lombok.Data;

import java.util.List;

@Data
public class NettyTestFilter implements ICorfuPayload<NettyTestFilter> {

    private final List<MsgFilter> messageFilters;

    public NettyTestFilter(List<MsgFilter> messageFilters) {
        this.messageFilters = messageFilters;
    }

    public NettyTestFilter(ByteBuf buf) {
        messageFilters = ICorfuPayload.listFromBuffer(buf, MsgFilter.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, messageFilters);
    }
}
