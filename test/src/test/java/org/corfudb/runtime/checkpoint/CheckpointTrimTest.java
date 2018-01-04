package org.corfudb.runtime.checkpoint;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 5/25/17.
 */
@CorfuTest
public class CheckpointTrimTest {

    @CorfuTest
    void testCheckpointTrim(CorfuRuntime runtime,
        @CorfuObjectParameter SMRMap<String, String> testMap,
        @CorfuObjectParameter(options = {ObjectOpenOptions.NO_CACHE})
            SMRMap<String, String> newTestMap) throws Exception {

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(testMap);
        long checkpointAddress = mcw.appendCheckpoints(runtime, "author");

        // Trim the log
        runtime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        // Ok, get a new view of the map
        // Reading an entry from scratch should be ok
        assertThat(newTestMap)
                .containsKeys("a", "b", "c");
    }

    @CorfuTest
    public void testSuccessiveCheckpointTrim(CorfuRuntime runtime,
        @CorfuObjectParameter SMRMap<String, String> testMap,
        @CorfuObjectParameter(options = {ObjectOpenOptions.NO_CACHE})
            SMRMap<String, String> newTestMap) throws Exception {
        final int nCheckpoints = 2;
        final long ckpointGap = 5;

        long checkpointAddress = -1;
        // generate two successive checkpoints
        for (int ckpoint = 0; ckpoint < nCheckpoints; ckpoint++) {
            // Place 3 entries into the map
            testMap.put("a", "a"+ckpoint);
            testMap.put("b", "b"+ckpoint);
            testMap.put("c", "c"+ckpoint);

            // Insert a checkpoint
            MultiCheckpointWriter mcw = new MultiCheckpointWriter();
            mcw.addMap(testMap);
            checkpointAddress = mcw.appendCheckpoints(runtime, "author");
        }

        // Trim the log in between the checkpoints
        runtime.getAddressSpaceView().prefixTrim(checkpointAddress - ckpointGap - 1);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        // Ok, get a new view of the map
        // try to get a snapshot inside the gap
        runtime.getObjectsView()
                .TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(checkpointAddress-1)
                .begin();

        // Reading an entry from scratch should be ok
        assertThat(newTestMap.get("a"))
                .isEqualTo("a"+(nCheckpoints-1));
    }


    @CorfuTest
    public void testCheckpointTrimDuringPlayback(CorfuRuntime runtime,
        @CorfuObjectParameter SMRMap<String, String> testMap,
        @CorfuObjectParameter(options = {ObjectOpenOptions.NO_CACHE})
            SMRMap<String, String> newTestMap) throws Exception {

        // Place 3 entries into the map
        testMap.put("a", "a");
        testMap.put("b", "b");
        testMap.put("c", "c");

        // Ok, get a new view of the map
        // Play the new view up to "b" only
        runtime.getObjectsView().TXBuild()
                .setType(TransactionType.SNAPSHOT)
                .setSnapshot(1)
                .begin();

        assertThat(newTestMap)
                .containsKeys("a", "b")
                .hasSize(2);

        runtime.getObjectsView().TXEnd();

        // Insert a checkpoint
        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(testMap);
        long checkpointAddress = mcw.appendCheckpoints(runtime, "author");

        // Trim the log
        runtime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
        runtime.getAddressSpaceView().gc();
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();


        // Sync should encounter trim exception, reset, and use checkpoint
        assertThat(newTestMap)
                 .containsKeys("a", "b", "c");
    }
}
