package org.corfudb.integration.cluster.tests;

import org.corfudb.integration.cluster.Harness.Harness;
import org.corfudb.integration.cluster.Harness.Node;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.corfudb.integration.cluster.Harness.Harness.run;

public class RebootTests {


    volatile long currentOp = 0;

    private Thread writer(Map<String, String> map, CorfuRuntime rt) {
        Runnable r = () -> {
            for (;;) {
                rt.getObjectsView().TXBegin();
                map.put(String.valueOf(currentOp), String.valueOf(currentOp));
                rt.getObjectsView().TXEnd();
                currentOp++;
            }
        };
        return new Thread(r);
    }

    private Thread statusCheck(CorfuRuntime rt) {
        Runnable r = () -> {
            long prevOp = -1;
            for (;;) {
                try {
                    rt.invalidateLayout();
                    System.out.println("current write: " + currentOp);
                    System.out.println("current tail: " + rt.getSequencerView().query());
                    System.out.println("layout: " + rt.getLayoutView().getLayout());
                    if (currentOp == prevOp) {
                        System.out.println("System stopped! ========================" );
                    }
                    prevOp = currentOp;
                    Thread.sleep(1000 * 5);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        return new Thread(r);
    }

    @Test
    public void forceRemoveTests() throws Exception {

        Harness harness = new Harness();
        int numNodes = 3;
        List<Node> nodeList = harness.deployCluster(numNodes);

        Node n0 = nodeList.get(0);
        Node n1 = nodeList.get(1);
        Node n2 = nodeList.get(2);


        CorfuRuntime rt = new CorfuRuntime(n2.getClusterAddress()).connect();
        Map<String, String> map = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();

        Thread writer = writer(map, rt);
        Thread status = statusCheck(rt);

        writer.start();
        status.start();

        Thread.sleep(1000 * 20);

        run(n0.shutdown, n1.shutdown);

        final Duration timeout = Duration.ofMinutes(5);
        final Duration pollPeriod = Duration.ofMillis(1000 * 10);
        final int workflowNumRetry = 3;

        CorfuRuntime rt2 = rt; //new CorfuRuntime(n2.getClusterAddress()).connect();
        rt2.getManagementView().forceRemoveNode(n0.getAddress(), workflowNumRetry, timeout, pollPeriod);
        rt2.getManagementView().forceRemoveNode(n1.getAddress(), workflowNumRetry, timeout, pollPeriod);

        System.out.println("yee");

        writer.join();
        status.join();
    }
}
