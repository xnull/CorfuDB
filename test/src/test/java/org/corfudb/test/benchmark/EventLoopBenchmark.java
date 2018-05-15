package org.corfudb.test.benchmark;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import lombok.Cleanup;
import lombok.Getter;
import net.openhft.affinity.Affinity;
import org.corfudb.protocols.wireprotocol.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EventLoopBenchmark extends AbstractCorfuBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {

        /**
         * A single boss group for the trial.
         */
        @Getter
        EventLoopGroup bossGroup;

        /**
         * A single worker group for the trial.
         */
        @Getter
        EventLoopGroup workerGroup;

        /**
         * A single client group for the trial.
         */
        @Getter
        EventLoopGroup clientGroup;

        ServerBootstrap bootstrap;
        Bootstrap clientBootstrap;

        SocketAddress localAddress;
        BenchmarkClientHandler clientHandler;

        @Setup(Level.Trial)
        public void init() {
            bossGroup = new DefaultEventLoopGroup(1,
                    new ThreadFactoryBuilder()
                            .setNameFormat("boss-%d")
                            .setDaemon(true)
                            .build());

            int numThreads = Runtime.getRuntime().availableProcessors() * 2;
            workerGroup = new DefaultEventLoopGroup(numThreads,
                    new ThreadFactoryBuilder()
                            .setNameFormat("worker-%d")
                            .setDaemon(true)
                            .build());

            clientGroup = new DefaultEventLoopGroup(numThreads,
                    new ThreadFactoryBuilder()
                            .setNameFormat("client-%d")
                            .setDaemon(true)
                            .build());

            bootstrap = new ServerBootstrap();
            localAddress = new LocalAddress("0");
            bootstrap.group(bossGroup, workerGroup)
                     .channel(LocalServerChannel.class);
            bootstrap.childHandler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldPrepender(4));
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer
                            .MAX_VALUE, 0, 4,
                            0, 4));
                    ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                    ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                    ch.pipeline().addLast(new BenchmarkServerHandler());
                }
            });
            bootstrap.bind(localAddress);

            clientBootstrap = new Bootstrap();
            clientBootstrap.group(clientGroup);
            clientBootstrap.channel(LocalChannel.class);
            clientHandler = new BenchmarkClientHandler();

            clientBootstrap.handler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ch.pipeline().addLast(new LengthFieldPrepender(4));
                    ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer
                            .MAX_VALUE, 0, 4,
                            0, 4));
                    ch.pipeline().addLast(new NettyCorfuMessageDecoder());
                    ch.pipeline().addLast(new NettyCorfuMessageEncoder());
                    ch.pipeline().addLast(clientHandler);
                }
            });
            clientBootstrap.connect(localAddress);
        }

        class BenchmarkServerHandler extends SimpleChannelInboundHandler<CorfuMsg> {

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, CorfuMsg msg) throws Exception {
                handleServerMessage(ctx, msg);
            }
        }

        void handleServerMessage(ChannelHandlerContext ctx, CorfuMsg msg) {
            CorfuMsg reply = CorfuMsgType.PONG.msg();
            reply.setRequestID(msg.getRequestID());
            ctx.writeAndFlush(reply);
        }


        class BenchmarkClientHandler extends  SimpleChannelInboundHandler<CorfuMsg> {

            CompletableFuture<ChannelHandlerContext> contextFuture = new CompletableFuture<>();
            AtomicLong counter = new AtomicLong(0L);
            Map<Long, CompletableFuture<CorfuMsg>> futures = new ConcurrentHashMap<>();

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, CorfuMsg msg) throws Exception {
                CompletableFuture cf = futures.get(msg.getRequestID());
                if (cf != null) {
                    cf.complete(msg);
                }
            }


            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                super.channelActive(ctx);
                contextFuture.complete(ctx);
            }

            void sendMessage(CorfuMsg msg) {
                contextFuture.join().writeAndFlush(msg);
            }

            public CompletableFuture<CorfuMsg> sendAndGetFuture(CorfuMsg msg) {
                msg.setRequestID(counter.getAndIncrement());
                CompletableFuture<CorfuMsg> cf = new CompletableFuture<>();
                futures.put(msg.getRequestID(), cf);
                sendMessage(msg);
                return cf;
            }
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            clientGroup.shutdownGracefully();
        }
    }

    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void send(BenchmarkState bs) {
        bs.clientHandler.sendAndGetFuture(CorfuMsgType.PING.msg()).join();
    }

    public static class BenchmarkStateWithExecutor extends BenchmarkState {

        ExecutorService ex;
        @Override
        public void init() {
            ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
            super.init();
        }

        @Override
        void handleServerMessage(ChannelHandlerContext ctx, CorfuMsg msg) {
            ex.execute(() -> {
                super.handleServerMessage(ctx, msg);
            });
        }

        @Override
        public void cleanup() {
            super.cleanup();
            ex.shutdown();
        }
    }

    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void sendWithFixedExecutor(BenchmarkStateWithExecutor bs) {
        bs.clientHandler.sendAndGetFuture(CorfuMsgType.PING.msg()).join();
    }



    public static class BenchmarkStateWithCachedExecutor extends BenchmarkState {

        ExecutorService ex;
        @Override
        public void init() {
            ex = Executors.newCachedThreadPool();
            super.init();
        }

        @Override
        void handleServerMessage(ChannelHandlerContext ctx, CorfuMsg msg) {
            ex.execute(() -> {
                super.handleServerMessage(ctx, msg);
            });
        }

        @Override
        public void cleanup() {
            super.cleanup();
            ex.shutdown();
        }
    }

    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void sendWithCachedExecutor(BenchmarkStateWithCachedExecutor bs) {
        bs.clientHandler.sendAndGetFuture(CorfuMsgType.PING.msg()).join();
    }


    public static class BenchmarkStateWithWorkStealingExecutor extends BenchmarkState {

        ExecutorService ex;
        @Override
        public void init() {
            ex = Executors.newWorkStealingPool();
            super.init();
        }

        @Override
        void handleServerMessage(ChannelHandlerContext ctx, CorfuMsg msg) {
            ex.execute(() -> {
                super.handleServerMessage(ctx, msg);
            });
        }

        @Override
        public void cleanup() {
            super.cleanup();
            ex.shutdown();
        }
    }

    @Benchmark
    @Warmup(iterations=5)
    @Measurement(iterations=10)
    @Threads(5)
    public void sendWithWorkStealingExecutor(BenchmarkStateWithCachedExecutor bs) {
        bs.clientHandler.sendAndGetFuture(CorfuMsgType.PING.msg()).join();
    }
}
