package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.Data;

/**
 * Representation of filtering action, corresponding corfu message type that the action
 * must be applied to, along with probability of applying the action.
 */
@Data
public class MsgFilter implements ICorfuPayload<MsgFilter> {

    public enum Action {
        ACCEPT,
        DROP
    }

    private final CorfuMsgType corfuMsgType;
    private final Action action;
    private final Double filterProbability;

    public MsgFilter(CorfuMsgType corfuMsgType, Action action,
                     Double filterProbability) {
        this.action = action;
        this.corfuMsgType = corfuMsgType;
        this.filterProbability = filterProbability;
    }

    public MsgFilter(ByteBuf buf) {
        action = Action.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        corfuMsgType = CorfuMsgType.valueOf(ICorfuPayload.fromBuffer(buf, String.class));
        filterProbability = ICorfuPayload.fromBuffer(buf, Double.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, action.toString());
        ICorfuPayload.serialize(buf, corfuMsgType.toString());
        ICorfuPayload.serialize(buf, filterProbability);
    }
}
