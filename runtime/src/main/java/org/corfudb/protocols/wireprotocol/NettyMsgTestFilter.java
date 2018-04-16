package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.Data;

@Data
public class NettyMsgTestFilter implements ICorfuPayload<NettyMsgTestFilter> {

    public enum FilterAction {
        ACCEPT,
        DROP
    }

    private final CorfuMsgType corfuMsgType;
    private final FilterAction filterAction;
    private final Double filterProbability;

    public NettyMsgTestFilter(CorfuMsgType corfuMsgType, FilterAction filterAction,
                              Double filterProbability) {
        this.filterAction = filterAction;
        this.corfuMsgType = corfuMsgType;
        this.filterProbability = filterProbability;
    }

    public NettyMsgTestFilter(ByteBuf buf) {
        filterAction = FilterAction.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        corfuMsgType = CorfuMsgType.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        filterProbability = ICorfuPayload.fromBuffer(buf, Double.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, filterAction.toString());
        ICorfuPayload.serialize(buf, corfuMsgType.toString());
        ICorfuPayload.serialize(buf, filterProbability);
    }
}
