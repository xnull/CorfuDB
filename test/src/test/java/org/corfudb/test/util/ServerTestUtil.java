package org.corfudb.test.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.netty.channel.Channel;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.ServerHandshakeHandler;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.UuidUtils;

public class ServerTestUtil {

    static void assertServerIsConnectedToClient(@Nonnull CorfuServer server, @Nonnull UUID client) {
        if (server
            .getServerContext().getClientChannels().values().stream()
            .noneMatch(ch -> ch.attr(ServerHandshakeHandler.CLIENT_ID).get()
                .equals(client))) {
            assertThat(false)
                .as("client %s"
                    + " is not connected to the server!", UuidUtils.asBase64(client))
                .isTrue();
        }
    }

    public static Channel getServerClientChannel(@Nonnull CorfuServer server,
                                                 @Nonnull CorfuRuntime runtime) {
        return getServerClientChannel(server, runtime.getParameters().getClientId());
    }

    public static Channel getServerClientChannel(@Nonnull CorfuServer server,
                                                 @Nonnull UUID client) {
        Optional<Channel> channel = server
            .getServerContext().getClientChannels().values().stream()
            .filter(ch ->
                ch.hasAttr(ServerHandshakeHandler.CLIENT_ID)
                && ch.attr(ServerHandshakeHandler.CLIENT_ID).get().equals(client))
            .findFirst();

        if (!channel.isPresent()) {
            assertThat(false)
                .as("Asked for server->client channel but client %s"
                    + " is not connected to the server!", UuidUtils.asBase64(client))
                .isTrue();
        }

        return channel.get();
    }

    public static void ifServerClientChannelPresent(@Nonnull CorfuServer server,
                                                    @Nonnull UUID client,
                                                    @Nonnull Consumer<Channel> channelConsumer) {
        Optional<Channel> channel = server
            .getServerContext().getClientChannels().values().stream()
            .filter(ch ->
                ch.hasAttr(ServerHandshakeHandler.CLIENT_ID)
                    && ch.attr(ServerHandshakeHandler.CLIENT_ID).get().equals(client))
            .findFirst();
        channel.ifPresent(channelConsumer::accept);
    }

    public static void disconnectAndBlacklistClientFromServer(@Nonnull UUID client,
                                                @Nonnull CorfuServer server) {
        server.getServerContext().getBlacklistedClients().add(client);
        ifServerClientChannelPresent(server, client, Channel::disconnect);
    }

    public static void disconnectAndBlacklistClientFromServer(@Nonnull CorfuRuntime runtime,
                                                @Nonnull CorfuServer server) {
        disconnectAndBlacklistClientFromServer(runtime.getParameters().getClientId(), server);
    }
}
