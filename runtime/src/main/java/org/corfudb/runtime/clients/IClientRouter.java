package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.NodeLocator;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static lombok.Builder.Default;

/**
 * This is an interface in which all client routers must implement.
 * Client routers are classes which talk to server routers. Clients are registered
 * on client routers using the addClient() interface, and can be retrieved using the
 * getClient() inteface.
 *
 * <p>Created by mwei on 12/13/15.
 */
public interface IClientRouter {


    /**
     * Add a new client to the router.
     *
     * @param client The client to add to the router.
     * @return This IClientRouter, to support chaining and the builder pattern.
     */
    IClientRouter addClient(IClient client);

    /**
     * Send a message and get a completable future to be fulfilled by the reply.
     *
     * @param ctx     The channel handler context to send the message under.
     * @param message The message to send.
     * @param <T>     The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    <T> CompletableFuture<T> sendMessageAndGetCompletable(ChannelHandlerContext ctx, CorfuMsg message);

    /**
     * Send a message using the router channel handler and
     * get a completable future to be fulfilled by the reply.
     *
     * @param message The message to send.
     * @param <T>     The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    default <T> CompletableFuture<T> sendMessageAndGetCompletable(CorfuMsg message) {
        return sendMessageAndGetCompletable(null, message);
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param ctx     The context to send the message under.
     * @param message The message to send.
     */
    void sendMessage(ChannelHandlerContext ctx, CorfuMsg message);

    /**
     * Send a one way message using the default channel handler,
     * without adding a completable future.
     *
     * @param message The message to send.
     */
    default void sendMessage(CorfuMsg message) {
        sendMessage(null, message);
    }

    /**
     * Send a netty message through this router, setting the fields in the outgoing message.
     *
     * @param ctx    Channel handler context to use.
     * @param inMsg  Incoming message to respond to.
     * @param outMsg Outgoing message.
     */
    void sendResponseToServer(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg);

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with
     * @param <T>        The type of the completion.
     */
    <T> void completeRequest(long requestID, T completion);

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    void completeExceptionally(long requestID, Throwable cause);

    /**
     * Stops routing requests.
     */
    void stop();

    /**
     * The host that this router is routing requests for.
     */
    String getHost();

    /**
     * The port that this router is routing requests for.
     */
    Integer getPort();

    @Builder
    @Getter
    class ClientRouterConfig {
        /**
         * {@link Duration} before requests timeout.
         */
        @Default
        @NonNull
        private final Duration requestTimeout = Duration.ofSeconds(5);

        /**
         * This timeout (in seconds) is used to detect servers that
         * shutdown abruptly without terminating the connection properly.
         */
        @Default
        int idleConnectionTimeout = 30;
        /**
         * The period at which the client sends keep-alive messages to the
         * server (a message is only send there is no write activity on the channel
         * for the whole period.
         */
        @Default
        int keepAlivePeriod = 10;
        /**
         * {@link Duration} before connections timeout.
         */
        @Default
        @NonNull
        Duration connectionTimeout = Duration.ofMillis(500);

        @Default
        private final Optional<SslContextConstructor.SslConfig> sslConfig = Optional.empty();

        /**
         * The {@link NodeLocator} which represents the remote node this
         * {@link NettyClientRouter} connects to.
         */
        @Getter
        @NonNull
        private final NodeLocator node;
    }

}
