package org.corfudb.runtime.clients;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.MsgFilter;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Sam Behnam on 5/18/18.
 */
@Slf4j
public class FilterInboundMsgHandler extends ChannelDuplexHandler {

    Map<CorfuMsgType, MsgFilter> msgTestFilterMap = new ConcurrentHashMap<>();

    public FilterInboundMsgHandler() {
        log.info("FilterInboundMsgHandler is initialized without any " +
                "initial filters.");
    }

    public FilterInboundMsgHandler(List<MsgFilter> msgFilterList) {
        registerFilterToMap(msgFilterList);
    }

    private void registerFilterToMap(List<MsgFilter> msgFilterList) {
        msgFilterList.forEach(corfuMsgFilter ->
                msgTestFilterMap.put(corfuMsgFilter.getCorfuMsgType(), corfuMsgFilter));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        CorfuMsg corfuMsg = (CorfuMsg) msg;
        // If a filtering message is read, then register it
        if (corfuMsg.getMsgType().equals(CorfuMsgType.MSG_FILTER_REQUEST)) {
            final MsgFilter filter = ((CorfuPayloadMsg<MsgFilter>) corfuMsg).getPayload();
            msgTestFilterMap.put(filter.getCorfuMsgType(), filter);
            return;
        }

        // Read the channel only if message is not blocked by filters
        if (!isMsgBlocked(corfuMsg)) {
            super.channelRead(ctx, msg);
        } else {
            log.info("Due to filtering rules, client router dropping inbound message." +
                    " id:{} and type: {}", corfuMsg.getRequestID(), corfuMsg.getMsgType());
        }
    }

    /** Evaluate the message against the configured rules. Returns true if applying the rule
     * results in dropping a message. Returns false if there is not rule configured or applying
     * the rule results in allow.
     *
     * @param msg
     * @return Whether the message must be drop
     */
    private boolean isMsgBlocked(CorfuMsg msg) {
        final MsgFilter msgFilter = msgTestFilterMap.get(msg.getMsgType());
        return msgFilter != null && msgFilter.getAction() == MsgFilter.Action.DROP;
    }
}
