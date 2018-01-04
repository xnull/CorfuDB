package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.ThreadParameter;

@CorfuTest
public class SnapshotTransactionTest implements ITransactionTest<SnapshotTransactionalContext> {

    @Override
    public void txBegin(CorfuRuntime runtime) {
        runtime.getObjectsView().TXBuild()
            .setType(TransactionType.SNAPSHOT)
            .begin();
    }

    public void txBegin(CorfuRuntime runtime, long snapshot) {
        runtime.getObjectsView().TXBuild()
            .setType(TransactionType.SNAPSHOT)
            .setSnapshot(snapshot)
            .begin();
    }

    public class SnapshotThread<K, V>
        extends TransactionThread<K, V, SnapshotTransactionTest> {

        void txBegin(int snapshot) {
            getTest().txBegin(getRuntime(), snapshot);
        }

    }

    /** Check if we can read a snapshot from the past, without
     * concurrent modifications.
     */
    @CorfuTest
    void snapshotReadable(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test", name = "map") SMRMap<String, String> map,
        @ThreadParameter(object = "map", name = "t1") SnapshotThread<String, String> t1) {

        t1.put("k" , "v1");    // TS = 0
        t1.put("k" , "v2");    // TS = 1
        t1.put("k" , "v3");    // TS = 2
        t1.put("k" , "v4");    // TS = 3

        t1.txBegin(2);
        assertThat(t1.get("k"))
            .isEqualTo("v3");
        t1.txEnd();
    }


    /** Ensure that a snapshot remains stable, even with
     * concurrent modifications.
     */
    @CorfuTest
    public void snapshotReadableWithConcurrentWrites(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test", name = "map") SMRMap<String, String> map,
        @ThreadParameter(object = "map", name = "t1") SnapshotThread<String, String> t1,
        @ThreadParameter(object = "map", name = "t2") SnapshotThread<String, String> t2,
        @ThreadParameter(object = "map", name = "t3") SnapshotThread<String, String> t3,
        @ThreadParameter(object = "map", name = "t4") SnapshotThread<String, String> t4) {

        t1.put("k" , "v1");    // TS = 0
        t2.put("k" , "v2");    // TS = 1
        t3.put("k" , "v3");    // TS = 2
        t4.put("k" , "v4");    // TS = 3

        t2.txBegin(2);
        assertThat(t2.get("k"))
            .isEqualTo("v3");
        t4.put("k" , "v4");    // TS = 4
        assertThat(t2.get("k"))
            .isEqualTo("v3");
        t2.txEnd();
    }

    /* Test if we can have implicit nested transaction for SnapshotTransactions. */
    @CorfuTest
    public void testSnapshotTxNestedImplicitTx(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test", name = "map") SMRMap<String, Integer> map,
        @ThreadParameter(object = "map", name = "t1") SnapshotThread<String, Integer> t1) {
        t1.put("a", 1);
        t1.put("b", 1);

        t1.txBegin(2);
        t1.forEach((k, v) -> {});
        t1.txEnd();
    }

    /* Test if we can have explicit nested transaction for SnapshotTransactions. */
    @CorfuTest
    public void testSnapshotTxNestedExplicitTx(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test", name = "map") SMRMap<String, Integer> map,
        @ThreadParameter(object = "map", name = "t1") SnapshotThread<String, Integer> t1) {
        t1.put("a", 1);
        t1.put("b", 1);

        t1.txBegin(2);
        t1.txBegin(2);
        t1.forEach((k, v) -> {});
        t1.txEnd();
        t1.txEnd();
    }

}
