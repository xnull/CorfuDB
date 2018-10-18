package org.corfudb.runtime.clients;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler;
import org.corfudb.protocols.wireprotocol.ClientHandshakeHandler.ClientHandshakeEvent;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.InboundMsgFilterHandler;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.ShutdownException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.security.sasl.SaslUtils;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyClient;
import org.corfudb.security.tls.SslContextConstructor;
import org.corfudb.util.CFUtils;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A client router which multiplexes operations over the Netty transport.
 *
 * <p>Created by mwei on 12/8/15.
 */
@Slf4j
@Sharable
public class NettyClientRouter extends SimpleChannelInboundHandler<CorfuMsg> implements IClientRouter {

    /**
     * The current request ID.
     */
    @Getter
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    private final AtomicLong requestID = new AtomicLong();
    /**
     * The handlers registered to this router.
     */
    private final ConcurrentMap<CorfuMsgType, IClient> handlerMap;

    /**
     * The outstanding requests on this router.
     */
    private final ConcurrentMap<Long, CompletableFuture<?>> outstandingRequests = new ConcurrentHashMap<>();

    /**
     * The {@link EventLoopGroup} for this router which services requests.
     */
    private final EventLoopGroup eventLoopGroup;

    /**
     * Whether to shutdown the {@code eventLoopGroup} or not. Only applies when
     * a deprecated constructor (which generates its own {@link EventLoopGroup} is used.
     */
    private boolean shutdownEventLoop = false;

    /**
     * Whether or not this router is shutdown.
     */
    private volatile boolean shutdown;

    /**
     * The {@link CorfuRuntimeParameters} used to configure the
     * router.
     */
    private final CorfuRuntimeParameters parameters;

    /**
     * A {@link CompletableFuture} which is completed when a connection,
     * including a successful handshake completes and messages can be sent
     * to the remote node.
     */
    @Getter
    private volatile CompletableFuture<Void> connectionFuture = new CompletableFuture<>();

    private SslContext sslContext;
    private final ImmutableMap<CorfuMsgType, String> timerNameCache;

    private final ClientRouterConfig routerConfig;

    public NettyClientRouter(@Nonnull NodeLocator node, @Nonnull ClientRouterConfig routerConfig) {
        this(
                node,
                parameters.getSocketType().getGenerator().generate(
                        Runtime.getRuntime().availableProcessors() * 2,
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                                .build()
                ),
                routerConfig
        );
        shutdownEventLoop = true;
    }

    /**
     * Creates a new NettyClientRouter connected to the specified host and port with the
     * specified tls and sasl options. The new {@link this} will attempt connection to
     * the node until {@link this#stop()} is called.
     *
     * @param node           The node to connect to.
     * @param eventLoopGroup The {@link EventLoopGroup} for servicing I/O.[]
     */
    public NettyClientRouter(@Nonnull NodeLocator node,
                             @Nonnull EventLoopGroup eventLoopGroup,
                             @Nonnull ClientRouterConfig routerConfig) {
        this.node = node;
        this.routerConfig = routerConfig;
        this.eventLoopGroup = eventLoopGroup;
        this.handlerMap = new ConcurrentHashMap<>();

        // Set timer mapping
        Builder<CorfuMsgType, String> mapBuilder = ImmutableMap.builder();
        for (CorfuMsgType type : CorfuMsgType.values()) {
            mapBuilder.put(type,
                    CorfuComponent.CLIENT_ROUTER.toString() + type.name().toLowerCase());
        }

        timerNameCache = mapBuilder.build();

        routerConfig.getSslConfig().ifPresent(sslConfig -> {
            try {
                sslContext = SslContextConstructor.constructSslContext(false,
                        sslConfig.getKeyStore(),
                        sslConfig.getKsPasswordFile(),
                        sslConfig.getTrustStore(),
                        sslConfig.getTsPasswordFile());
            } catch (SSLException e) {
                throw new UnrecoverableCorfuError(e);
            }
        });

        addClient(new BaseHandler());


        // Initialize the channel
        Bootstrap b = new Bootstrap();
        b.group(eventLoopGroup);
        b.channel(parameters.getSocketType().getChannelClass());
        parameters.getNettyChannelOptions().forEach(b::option);
        b.handler(getChannelInitializer());
        b.option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                (int) routerConfig.getConnectionTimeout().toMillis()
        );

        // Asynchronously connect, retrying until shut down.
        // Once connected, connectionFuture will be completed.
        connectAsync(b);
    }

    /**
     * Add a new client to the router.
     *
     * @param client The client to add to the router.
     * @return This NettyClientRouter, to support chaining and the builder pattern.
     */
    public IClientRouter addClient(IClient client) {
        // Set the client's router to this instance.
        client.setRouter(this);

        // Iterate through all types of CorfuMsgType, registering the handler
        client.getHandledTypes()
                .forEach(x -> {
                    handlerMap.put(x, client);
                    log.trace("Registered {} to handle messages of type {}", client, x);
                });

        return this;
    }

    /**
     * Get the {@link ChannelInitializer} used for initializing the Netty channel pipeline.
     *
     * @return A {@link ChannelInitializer} which initializes the pipeline.
     */
    private ChannelInitializer<Channel> getChannelInitializer() {
        return new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(@Nonnull Channel ch) {
                IdleStateHandler idleStateHandler = new IdleStateHandler(
                        routerConfig.getIdleConnectionTimeout(),
                        routerConfig.getKeepAlivePeriod(),
                        0
                );

                ch.pipeline().addLast(idleStateHandler);

                if (routerConfig.isTlsEnabled()) {
                    ch.pipeline().addLast("ssl", sslContext.newHandler(ch.alloc()));
                }

                ch.pipeline().addLast(new LengthFieldPrepender(4));
                ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));

                if (routerConfig.isSaslPlainTextEnabled()) {
                    PlainTextSaslNettyClient saslNettyClient = SaslUtils.enableSaslPlainText(
                            routerConfig.getUsernameFile(),
                            routerConfig.getPasswordFile()
                    );

                    ch.pipeline().addLast("sasl/plain-text", saslNettyClient);
                }

                ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                ch.pipeline().addLast(new NettyCorfuMessageEncoder());

                ClientHandshakeHandler clientHandshakeHandler = new ClientHandshakeHandler(
                        parameters.getClientId(), node.getNodeId(), parameters.getHandshakeTimeout()
                );
                ch.pipeline().addLast(clientHandshakeHandler);

                // If parameters include message filters, add corresponding filter handler
                if (parameters.getNettyClientInboundMsgFilters() != null) {
                    InboundMsgFilterHandler inboundMsgFilterHandler =
                            new InboundMsgFilterHandler(parameters.getNettyClientInboundMsgFilters());
                    ch.pipeline().addLast(inboundMsgFilterHandler);
                }

                ch.pipeline().addLast(NettyClientRouter.this);
            }
        };
    }

    /**
     * Add a future which reconnects the server.
     *
     * @param channel   The channel to use
     * @param bootstrap The channel bootstrap to use
     */
    private void addReconnectionOnCloseFuture(@Nonnull Channel channel, @Nonnull Bootstrap bootstrap) {
        channel.closeFuture().addListener(r -> {
            log.info("addReconnectionOnCloseFuture[{}]: disconnected", node);
            // Remove the current completion future, forcing clients to wait for reconnection.
            connectionFuture = new CompletableFuture<>();
            // Exceptionally complete all requests that were waiting for a completion.
            outstandingRequests.forEach((reqId, reqCompletableFuture) -> {
                reqCompletableFuture.completeExceptionally(new NetworkException("Disconnected", node));
                // And also remove them.
                outstandingRequests.remove(reqId);
            });
            // If we aren't shutdown, reconnect.
            if (!shutdown) {
                log.info("addReconnectionOnCloseFuture[{}]: reconnecting", node);
                // Asynchronously connect again.
                connectAsync(bootstrap);
            }
        });
    }

    /**
     * Connect to a remote server asynchronously.
     *
     * @param bootstrap The channel boostrap to use
     * @return A {@link ChannelFuture} which is c
     */
    private ChannelFuture connectAsync(@Nonnull Bootstrap bootstrap) {
        // Use the bootstrap to create a new channel.
        ChannelFuture f = bootstrap.connect(node.getHost(), node.getPort());
        f.addListener((ChannelFuture cf) -> channelConnectionFutureHandler(cf, bootstrap));
        return f;
    }

    /**
     * Handle when a channel is connected.
     *
     * @param future    The future that is completed when the channel is connected/
     * @param bootstrap The bootstrap to connect a new channel (used on reconnect).
     */
    private void channelConnectionFutureHandler(@Nonnull ChannelFuture future, @Nonnull Bootstrap bootstrap) {
        if (future.isSuccess()) {
            // Register a future to reconnect in case we get disconnected
            addReconnectionOnCloseFuture(future.channel(), bootstrap);
            log.info("connectAsync[{}]: Channel connected.", node);
            return;
        }
        // Otherwise, the connection failed. If we're not shutdown, try reconnecting after
        // a sleep period.
        if (!shutdown) {
            log.info("connectAsync[{}]: Channel connection failed, reconnecting...", node);
            Sleep.sleepUninterruptibly(parameters.getConnectionRetryRate());
            // Call connect, which will retry the call again.
            // Note that this is not recursive, because it is called in the
            // context of the handler future.
        }

        if (!shutdown) {
            connectAsync(bootstrap);
        }
    }

    /**
     * Stops routing requests.
     */
    @Override
    public void stop() {
        log.debug("stop: Shutting down router for {}", node);
        shutdown = true;
        connectionFuture.completeExceptionally(new ShutdownException());

        try {
            if (shutdownEventLoop) {
                eventLoopGroup.shutdownGracefully().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnrecoverableCorfuInterruptedError("Interrupted while stopping", e);
        }
    }

    /**
     * Send a message and get a completable future to be fulfilled by the reply.
     *
     * @param ctx     The channel handler context to send the message under.
     * @param message The message to send.
     * @param <T>     The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    public <T> CompletableFuture<T> sendMessageAndGetCompletable(@NonNull ChannelHandlerContext ctx, @NonNull CorfuMsg message) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();

        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        try {
            connectionFuture.get(routerConfig.getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UnrecoverableCorfuInterruptedError(e);
        } catch (TimeoutException | ExecutionException e) {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }

        // Set up the timer and context to measure request
        final Timer roundTripMsgTimer = CorfuRuntime
                .getDefaultMetrics()
                .timer(timerNameCache.get(message.getMsgType()));

        final Timer.Context roundTripMsgContext = MetricsUtils.getConditionalContext(isEnabled, roundTripMsgTimer);

        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();

        // Set the message fields.
        message.setClientID(parameters.getClientId());
        message.setRequestID(thisRequest);

        // Generate a future and put it in the completion table.
        final CompletableFuture<T> cf = new CompletableFuture<>();
        outstandingRequests.put(thisRequest, cf);

        // Write the message out to the channel.
        ChannelFuture f = ctx.writeAndFlush(message, ctx.voidPromise());
        f.addListener(ChannelFutureListener.CLOSE);

        log.trace("Sent message: {}", message);

        // Generate a benchmarked future to measure the underlying request
        final CompletableFuture<T> cfBenchmarked = cf.thenApply(x -> {
            MetricsUtils.stopConditionalContext(roundTripMsgContext);
            return x;
        });

        // Generate a timeout future, which will complete exceptionally
        // if the main future is not completed.
        final CompletableFuture<T> cfTimeout = CFUtils.within(
                cfBenchmarked,
                routerConfig.getRequestTimeout()
        );
        cfTimeout.exceptionally(e -> {
            outstandingRequests.remove(thisRequest);
            log.debug("Remove request {} to {} due to timeout! Message:{}", thisRequest, node, message);
            return null;
        });

        return cfTimeout;
    }

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param ctx     The context to send the message under.
     * @param message The message to send.
     */
    public void sendMessage(ChannelHandlerContext ctx, CorfuMsg message) {
        // Get the next request ID.
        final long thisRequest = requestID.getAndIncrement();
        // Set the base fields for this message.
        message.setClientID(parameters.getClientId());
        message.setRequestID(thisRequest);
        // Write this message out on the channel.
        Channel channel = ctx.channel();
        channel.writeAndFlush(message, channel.voidPromise());
        log.trace("Sent one-way message: {}", message);
    }


    /**
     * Send a netty message through this router, setting the fields in the outgoing message.
     *
     * @param ctx    Channel handler context to use.
     * @param inMsg  Incoming message to respond to.
     * @param outMsg Outgoing message.
     */
    public void sendResponseToServer(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        ctx.writeAndFlush(outMsg, ctx.voidPromise());
        log.trace("Sent response: {}", outMsg);
    }

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestId  The request to complete.
     * @param completion The value to complete the request with
     * @param <T>        The type of the completion.
     */
    public <T> void completeRequest(long requestId, T completion) {
        CompletableFuture<T> cf;
        if ((cf = (CompletableFuture<T>) outstandingRequests.get(requestId)) != null) {
            cf.complete(completion);
            outstandingRequests.remove(requestId);
        } else {
            log.warn("Attempted to complete request {}, but request not outstanding!", requestId);
        }
    }

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    public void completeExceptionally(long requestID, Throwable cause) {
        CompletableFuture<?> cf;
        if ((cf = outstandingRequests.get(requestID)) != null) {
            cf.completeExceptionally(cause);
            outstandingRequests.remove(requestID);
        } else {
            log.warn("Attempted to exceptionally complete request {}, but request not outstanding!", requestID);
        }
    }

    /**
     * Validate the clientID of a CorfuMsg.
     *
     * @param msg The incoming message to validate.
     * @return True, if the clientID is correct, but false otherwise.
     */
    private boolean isClientIdValid(CorfuMsg msg) {
        // Check if the message is intended for us. If not, drop the message.
        boolean validClient = msg.getClientID().equals(parameters.getClientId());

        if (!validClient) {
            String errMessage = "Incoming message intended for client {}, our id is {}, dropping!";
            log.warn(errMessage, msg.getClientID(), parameters.getClientId());
        }

        return validClient;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CorfuMsg msg) {
        // We get the handler for this message from the map
        IClient handler = handlerMap.get(msg.getMsgType());
        if (handler == null) {
            log.warn("Received unregistered message {}, dropping", msg);
            return;
        }

        if (!isClientIdValid(msg)) {
            return;
        }

        // Route the message to the handler.
        log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), msg);
        try {
            handler.handleMessage(msg, ctx);
        } catch (Exception e) {
            log.error("Exception during read!", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Exception during channel handling.", cause);
        ctx.close();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    /**
     * Sends a ping to the server so that the pong response will keep
     * the channel active in order to avoid a ReadTimeout exception that will
     * close the channel.
     */
    private void keepAlive(ChannelHandlerContext ctx) {
        Channel channel = ctx.channel();
        if (!channel.isOpen()) {
            log.warn("keepAlive: channel not open, skipping ping. ");
            return;
        }
        sendMessageAndGetCompletable(ctx, new CorfuMsg(CorfuMsgType.PING));
        log.trace("keepAlive: sending ping to {}", channel.remoteAddress());
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt.equals(ClientHandshakeEvent.CONNECTED)) {
            // Handshake successful. Complete the connection future to allow
            // clients to proceed.
            connectionFuture.complete(null);
        } else if (evt.equals(ClientHandshakeEvent.FAILED) && connectionFuture.isDone()) {
            // Handshake failed. If the current completion future is complete,
            // create a new one to unset it, causing future requests
            // to wait.
            connectionFuture = new CompletableFuture<>();
        } else if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            if (e.state() == IdleState.READER_IDLE) {
                ctx.close();
            } else if (e.state() == IdleState.WRITER_IDLE) {
                keepAlive(ctx);
            }
        } else {
            log.warn("userEventTriggered: unhandled event {}", evt);
        }
    }

    @Deprecated
    @Override
    public Integer getPort() {
        return node.getPort();
    }

    @Deprecated
    public String getHost() {
        return node.getHost();
    }
}
