package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT  {

    @Test
    public void restartServer() throws Exception {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        BaseClient client = rt.getLayoutView().getRuntimeLayout().getBaseClient("localhost:9000");

        Thread r = new Thread(() -> {
            while (true) {
                try {
                    client.ping();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });

        Thread r2 = new Thread(() -> {
            while (true) {
                try {
                    client.ping();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        });
        Thread r3 = new Thread(() -> {
            while (true) {
                try {
                    client.ping();
                } catch (Exception e) {
                    //System.out.println(e);
                }
            }
        });

        r.start();
        r2.start();
        r3.start();

        Thread.sleep(1000 * 3);


        for (int x = 0; x < 6; x++) {
            System.out.println("iter  " + x);
           // rt.getNettyEventLoop().shutdownGracefully().sync();
            rt.getNettyEventLoop().shutdownGracefully().syncUninterruptibly();
            rt.shutdown();
        }

        r.join();
        r2.join();
        r3.join();
        //client.restart();
        //client.restart();
        //client.restart();
    }

    @Test
    public void restartServer2() throws Exception {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();
        BaseClient client = rt.getLayoutView().getRuntimeLayout().getBaseClient("localhost:9000");
        client.restart();
        client.restart();
        client.restart();
    }

    @Test
    public void largeStreamWrite() throws Exception {
        //CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        CorfuRuntime[] rts = new CorfuRuntime[20];
        SequencerClient[] clients = new SequencerClient[rts.length];
        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime("localhost:9000").connect();
            clients[x] = rts[x].getLayoutView().getRuntimeLayout().getPrimarySequencerClient();
        }

        CompletableFuture[] futures = new CompletableFuture[5000];

        while (true) {
            for (int x = 0; x < futures.length; x++) {
                int id = ThreadLocalRandom.current().nextInt(rts.length);
                futures[x] = clients[id].nextToken(Collections.EMPTY_LIST, 0);
            }

            for (int x = 0; x < futures.length; x++) {
                try {
                    futures[x].get();
                } catch (Exception e) {
                    System.out.println(e);
                    for (int xa = 0; xa < rts.length; xa++) {
                        rts[xa].invalidateLayout();
                        clients[xa] = rts[xa].getLayoutView().getRuntimeLayout().getPrimarySequencerClient();
                    }
                }
            }
        }



    }
}
