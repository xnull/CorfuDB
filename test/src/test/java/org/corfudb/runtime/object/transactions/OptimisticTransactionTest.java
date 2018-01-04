package org.corfudb.runtime.object.transactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LOW;
import static org.corfudb.test.parameters.Param.TIMEOUT_LONG;

import com.google.common.reflect.TypeToken;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ConflictParameterClass;
import org.corfudb.runtime.object.TransactionalObject;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.Parameter;
import org.corfudb.test.parameters.ThreadParameter;
import org.corfudb.util.serializer.ICorfuHashable;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

@CorfuTest
@Slf4j
public class OptimisticTransactionTest
    implements ITransactionTest<OptimisticTransactionalContext> {

    @Override
    public void txBegin(CorfuRuntime runtime) {
        runtime.getObjectsView().TXBuild()
            .setType(TransactionType.OPTIMISTIC)
            .begin();
    }

    public class OptimisticThread<K, V>
        extends TransactionThread<K, V, OptimisticTransactionTest> {

    }

    /** Checks that the fine-grained conflict set is correctly produced
     * by the annotation framework.
     */
    @CorfuTest
    public void checkConflictParameters(CorfuRuntime runtime,
        @CorfuObjectParameter ConflictParameterClass testObject) {

        final String TEST_0 = "0";
        final String TEST_1 = "1";
        final int TEST_2 = 2;
        final int TEST_3 = 3;
        final String TEST_4 = "4";
        final String TEST_5 = "5";

        runtime.getObjectsView().TXBegin();
        // RS=TEST_0
        testObject.accessorTest(TEST_0, TEST_1);
        // WS=TEST_3
        testObject.mutatorTest(TEST_2, TEST_3);
        // WS,RS=TEST_4
        testObject.mutatorAccessorTest(TEST_4, TEST_5);

        // Assert that the conflict set contains TEST_0, TEST_4
        assertThat(TransactionalContext.getCurrentContext()
            .getReadSetInfo().getConflicts()
            .values()
            .stream()
            .flatMap(x -> x.stream())
            .collect(Collectors.toList()))
            .contains(TEST_0, TEST_4);

        // in optimistic mode, assert that the conflict set does NOT contain TEST_2, TEST_3
        assertThat(TransactionalContext.getCurrentContext()
            .getReadSetInfo()
            .getConflicts().values().stream()
            .flatMap(x -> x.stream())
            .collect(Collectors.toList()))
            .doesNotContain(TEST_2, TEST_3, TEST_5);

        runtime.getObjectsView().TXAbort();
    }


    @Data
    @AllArgsConstructor
    static class CustomConflictObject {
        final String k1;
        final String k2;
    }

    /** When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently, but when they conflict they should abort.
     */
    @CorfuTest
    public void customConflictObjectsConflictAborts(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<CustomConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<CustomConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<CustomConflictObject, String> t2)
    {
        CustomConflictObject c1 =
            new CustomConflictObject("a", "a");
        CustomConflictObject c2 =
            new CustomConflictObject("a", "a");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatThrownBy(t2::txEnd)
            .isInstanceOf(TransactionAbortedException.class);
    }

    /** When using two custom conflict objects which
     * do not provide a serializable implementation,
     * the implementation should hash them
     * transparently so they do not abort.
     */
    @CorfuTest
    public void customConflictObjectsNoConflictNoAbort(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<CustomConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<CustomConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<CustomConflictObject, String> t2)
    {
       CustomConflictObject c1 =
            new CustomConflictObject("a", "a");
        CustomConflictObject c2 =
            new CustomConflictObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatCode(t2::txEnd)
            .doesNotThrowAnyException();
    }


    @Data
    @AllArgsConstructor
    static class CustomSameHashConflictObject {
        final String k1;
        final String k2;
    }

    /** This test generates a custom object which has been registered
     * with the serializer to always conflict, as it always hashes to
     * and empty byte array.
     */
    @CorfuTest
    public void customSameHashAlwaysConflicts(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<CustomSameHashConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<CustomSameHashConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<CustomSameHashConflictObject, String> t2) {
        // Register a custom hasher which always hashes to an empty byte array
        Serializers.JSON.registerCustomHasher(CustomSameHashConflictObject.class, o -> new byte[0]);

        CustomSameHashConflictObject c1 = new CustomSameHashConflictObject("a", "a");
        CustomSameHashConflictObject c2 = new CustomSameHashConflictObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatThrownBy(t2::txEnd)
            .isInstanceOf(TransactionAbortedException.class);
    }

    @Data
    @AllArgsConstructor
    static class CustomHashConflictObject {
        final String k1;
        final String k2;
    }


    /** This test generates a custom object which has been registered
     * with the serializer to use the full value as the conflict hash,
     * and should not conflict.
     */
    @CorfuTest
    public void customHasDoesNotConflict(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<CustomHashConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<CustomHashConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<CustomHashConflictObject, String> t2) {
        // Register a custom hasher which always hashes to the two strings together as a
        // byte array
        Serializers.JSON.registerCustomHasher(CustomSameHashConflictObject.class,
            o -> {
                ByteBuffer b = ByteBuffer.wrap(new byte[o.k1.length() + o.k2.length()]);
                b.put(o.k1.getBytes());
                b.put(o.k2.getBytes());
                return b.array();
            });

        CustomHashConflictObject c1 = new CustomHashConflictObject("a", "a");
        CustomHashConflictObject c2 = new CustomHashConflictObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatCode(t2::txEnd)
            .doesNotThrowAnyException();
    }

    @Data
    @AllArgsConstructor
    static class IHashAlwaysConflictObject implements ICorfuHashable {
        final String k1;
        final String k2;

        @Override
        public byte[] generateCorfuHash() {
            return new byte[0];
        }
    }

    /** This test generates a custom object which implements an interface which always
     * conflicts.
     */
    @CorfuTest
    public void IHashAlwaysConflicts(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<IHashAlwaysConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<IHashAlwaysConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<IHashAlwaysConflictObject, String> t2) {
        IHashAlwaysConflictObject c1 = new IHashAlwaysConflictObject("a", "a");
        IHashAlwaysConflictObject c2 = new IHashAlwaysConflictObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatThrownBy(t2::txEnd)
            .isInstanceOf(TransactionAbortedException.class);
    }


    @Data
    @AllArgsConstructor
    static class IHashConflictObject implements ICorfuHashable {
        final String k1;
        final String k2;

        @Override
        public byte[] generateCorfuHash() {
            ByteBuffer b = ByteBuffer.wrap(new byte[k1.length() + k2.length()]);
            b.put(k1.getBytes());
            b.put(k2.getBytes());
            return b.array();
        }
    }

    /** This test generates a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @CorfuTest
    public void IHashNoConflicts(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<IHashConflictObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<IHashConflictObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<IHashConflictObject, String> t2) {
        IHashConflictObject c1 = new IHashConflictObject("a", "a");
        IHashConflictObject c2 = new IHashConflictObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatCode(t2::txEnd)
            .doesNotThrowAnyException();
    }

    @Data
    static class ExtendedIHashObject extends IHashConflictObject {

        public ExtendedIHashObject(String k1, String k2) {
            super(k1, k2);
        }

        /** A simple dummy method. */
        public String getK1K2() {
            return k1 + k2;
        }
    }

    /** This test extends a custom object which implements the CorfuHashable
     * interface and should not conflict.
     */
    @CorfuTest
    public void ExtendedIHashNoConflicts(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map")
            SMRMap<ExtendedIHashObject, String> map,
        @ThreadParameter(name = "t1", object = "map")
            OptimisticThread<ExtendedIHashObject, String> t1,
        @ThreadParameter(name = "t2", object = "map")
            OptimisticThread<ExtendedIHashObject, String> t2) {
        ExtendedIHashObject c1 = new ExtendedIHashObject("a", "a");
        ExtendedIHashObject c2 = new ExtendedIHashObject("a", "b");

        t1.txBegin();
        t2.txBegin();
        t1.put(c1 , "v1");
        t2.put(c2 , "v2");
        t1.txEnd();
        assertThatCode(t2::txEnd)
            .doesNotThrowAnyException();
    }

    /** In an optimistic transaction, we should be able to
     *  read our own writes in the same thread.
     */
    @CorfuTest
    public void readOwnWrites(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1)
    {
        t1.txBegin();
        t1.put("k" , "v");
        assertThat(t1.get("k"))
            .isEqualTo("v");
        t1.txEnd();
    }

    /** We should not be able to read writes written optimistically
     * by other threads.
     */
    @CorfuTest
    public void otherThreadCannotReadOptimisticWrites(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1,
        @ThreadParameter(name = "t2", object = "map") OptimisticThread<String, String> t2)
    {
        t1.txBegin();
        t2.txBegin();
        // T1 inserts k,v1 optimistically. Other threads
        // should not see this optimistic put.
        t1.put("k", "v1");
        // T2 now reads k. It should not see T1's write.
        assertThat(t2.get("k"))
            .isNull();
        // T2 inserts k,v2 optimistically. T1 should not
        // be able to see this write.
        t2.put("k", "v2");
        // T1 now reads "k". It should not see T2's write.
        assertThat(t1.get("k"))
            .isNotEqualTo("v2");
    }

    /** This test ensures if modifying multiple keys, with one key that does
     * conflict and another that does not, causes an abort.
     */
    @CorfuTest
    public void modifyingMultipleKeysCausesAbort(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1,
        @ThreadParameter(name = "t2", object = "map") OptimisticThread<String, String> t2) {
        // T1 modifies k1 and k2.
        t1.txBegin();
        t1.put("k1", "v1");
        t1.put("k2", "v2");

        // T2 modifies k1, commits
        t2.txBegin();
        t2.put("k1", "v3");
        t2.txEnd();

        // T1 commits, should abort
        assertThatThrownBy(t1::txEnd)
            .isInstanceOf(TransactionAbortedException.class);
    }

    /** Ensure that, upon two consecutive nested transactions, the latest transaction can
     * see optimistic updates from previous ones.
     *
     */
    @CorfuTest
    public void OptimisticStreamGetUpdatedCorrectlyWithNestedTransaction(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1){
        t1.txBegin();
        t1.put("k", "v0");

        // Start first nested transaction
        t1.txBegin();
        assertThat(t1.get("k"))
            .isEqualTo("v0");
        t1.put("k", "v1");
        t1.txEnd();
        // End first nested transaction

        // Start second nested transaction
        t1.txBegin();
        assertThat(t1.get("k"))
            .isEqualTo("v1");
        t1.put("k", "v2");
        t1.txEnd();
        // End second nested transaction

        assertThat(t1.get("k"))
            .isEqualTo("v2");
        t1.txEnd();
        assertThat(map)
            .containsEntry("k", "v2");

    }

    /** Threads that start a transaction at the same time
     * (with the same timestamp) should cause one thread
     * to abort while the other succeeds.
     */
    @CorfuTest
    public void threadShouldAbortAfterConflict(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1,
        @ThreadParameter(name = "t2", object = "map") OptimisticThread<String, String> t2)
    {
        // T1 starts non-transactionally.
        t1.put("k", "v0");
        t1.put("k1", "v1");
        t1.put("k2", "v2");
        // Now T1 and T2 both start transactions and read v0.
        t1.txBegin();
        t2.txBegin();
        assertThat(t1.get("k"))
            .isEqualTo("v0");
        assertThat(t2.get("k"))
            .isEqualTo("v0");
        // Now T1 modifies k -> v1 and commits.
        t1.put("k", "v1");
        t1.txEnd();
        // And T2 modifies k -> v2 and tries to commit, but
        // should abort.
        t2.put("k", "v2");
        assertThatThrownBy(t2::txEnd)
            .isInstanceOf(TransactionAbortedException.class);
        // At the end of the transaction, the map should only
        // contain T1's modification.
        assertThat(map)
            .containsEntry("k", "v1");
    }

    /** This test makes sure that a single thread can read
     * its own nested transactions after they have committed,
     * and that nested transactions are committed with the
     * parent transaction.
     */
    @CorfuTest
    public void nestedTransactionsCanBeReadDuringCommit(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1) {
        // We start without a transaction and put k,v1
        t1.put("k", "v1");
        // Now we start a transaction and put k,v2
        t1.txBegin();
        assertThat(t1.put("k", "v2")) // put should return the previous value
            .isEqualTo("v1"); // which is v1.
        // Now we start a nested transaction. It should
        // read v2.
        t1.txBegin();
        assertThat(t1.get("k"))
            .isEqualTo("v2");
        // Now we put k,v3
        assertThat(t1.put("k", "v3"))
            .isEqualTo("v2");   // previous value = v2
        // And then we commit.
        t1.txEnd();
        // And we should be able to read the nested put
        assertThat(t1.get("k"))
            .isEqualTo("v3");
        // And we commit the parent transaction.
        t1.txEnd();

        // And now k,v3 should be in the map.
        assertThat(map)
            .containsEntry("k", "v3");
    }

    /** This test makes sure that the nested transactions
     * of two threads are not visible to each other.
     */
    @CorfuTest
    public void nestedTransactionsAreIsolatedAcrossThreads(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1,
        @ThreadParameter(name = "t2", object = "map") OptimisticThread<String, String> t2) {
        // Start a transaction on both threads.
        t1.txBegin();
        t2.txBegin();
        // Put k, v1 on T1 and k, v2 on T2.
        t1.put("k", "v1");
        t2.put("k", "v2");
        // Now, start a nested transaction on both threads.
        t1.txBegin();
        t2.txBegin();
        // T1 should see v1 and T2 should see v2.
        assertThat(t1.get("k"))
            .isEqualTo("v1");
        assertThat(t2.get("k"))
            .isEqualTo("v2");
        // Now we put k,v3 on T1 and k,v4 on T2
        t1.put("k", "v3");
        t2.put("k", "v4");
        // And each thread should only see its own modifications.
        assertThat(t1.get("k"))
            .isEqualTo("v3");
        assertThat(t2.get("k"))
            .isEqualTo("v4");
        // Now we exit the nested transaction. They should both
        // commit, because they are in optimistic mode.
        t1.txEnd();
        t2.txEnd();
        // Check that the parent transaction can only
        // see the correct modifications.
        assertThat(t1.get("k"))
            .isEqualTo("v3");
        assertThat(t2.get("k"))
            .isEqualTo("v4");
        // Commit the parent transactions. T2 should abort
        // due to concurrent modification with T1.
        t1.txEnd();
        assertThatThrownBy(t2::txEnd)
            .isInstanceOf(TransactionAbortedException.class);

        // And the map should contain k,v3 - T1's update.
        assertThat(map)
            .containsEntry("k", "v3")
            .doesNotContainEntry("k", "v4");
    }

    /**
     * Check that on abortion of a nested transaction
     * the modifications that happened within it are not
     * leaked into the parent transaction.
     */
    @CorfuTest
    public void nestedTransactionCanBeAborted(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1) {
        t1.txBegin();
        t1.put("k", "v1");
        assertThat(t1.get("k"))
            .isEqualTo("v1");
        t1.txBegin();
        t1.put("k", "v2");
        assertThat(t1.get("k"))
            .isEqualTo("v2");
        t1.txAbort();
        assertThat(t1.get("k"))
            .isEqualTo("v1");
        t1.txEnd();
        assertThat(map)
            .containsEntry("k", "v1");
    }

    /** This test makes sure that a write-only transaction properly
     * commits its updates, even if there are no accesses
     * during the transaction.
     */
    @CorfuTest
    public void writeOnlyTransactionCommitsInMemory(CorfuRuntime runtime,
        @CorfuObjectParameter(name = "map") SMRMap<String, String> map,
        @ThreadParameter(name = "t1", object = "map") OptimisticThread<String, String> t1) {
        // Write twice to the transaction without a read
        t1.txBegin();
        t1.write("k", "v1");
        t1.write("k", "v2");
        t1.txEnd();

        // Make sure the object correctly reflects the value
        // of the most recent write.
        assertThat(map)
            .containsEntry("k", "v2");
    }


    /** This test checks if modifying two keys from
     *  two different streams will cause a collision.
     *
     *  In the old single-level design, this would cause
     *  a collision since 16 bits of the stream id were
     *  being hashed into the 32 bit hashCode(), so that
     *  certain stream-conflictkey combinations would
     *  collide, as demonstrated below.
     *
     *  TODO: Potentially remove this unit test
     *  TODO: once the hash function has stabilized.
     */
    @CorfuTest
    public void collide16Bit(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test-1", name = "map1") SMRMap<String, String> map1,
        @CorfuObjectParameter(stream = "test-2", name = "map2") SMRMap<String, String> map2,
        @ThreadParameter(name = "t1", object = "map1") OptimisticThread<String, String> t1,
        @ThreadParameter(name = "t2", object = "map2") OptimisticThread<String, String> t2)
        throws Exception {
        t1.txBegin();
        t2.txBegin();
        t1.put("azusavnj", "1");
        t2.put("ajkenmbb", "2");
        t1.txEnd();
        assertThatCode(t2::txEnd)
            .doesNotThrowAnyException();
    }


    /**
     * Implicit transaction should not be nested. All implicit Tx
     * end up in TXExecute, so here we execute isInNestedTransaction
     * in TXExecute to check that we are indeed not in a nested tx.
     *
     * @throws Exception
     */
    @CorfuTest
    public void implicitTransactionalMethodAreNotNested(CorfuRuntime runtime,
        @CorfuObjectParameter TransactionalObject to) {

        txBegin(runtime);
        assertThat(to.isInTransaction()).isTrue();
        assertThat(to.isInNestedTransaction()).isFalse();
        txEnd(runtime);
    }

    /**
     * Implicit transaction should be in a transaction. If we are not
     * in a transaction already, it should create a new transaction,.
     *
     * @throws Exception
     */
    @CorfuTest
    public void implicitTransactionalMethodIsInTransaction(CorfuRuntime runtime,
        @CorfuObjectParameter TransactionalObject to) {
        assertThat(to.isInTransaction()).isTrue();
    }

    /**
     * Runtime exception thrown during "nested" transaction should
     * abort the transaction.
     */
    @CorfuTest
    public void runtimeExceptionAbortNestedTransaction(CorfuRuntime runtime,
        @CorfuObjectParameter TransactionalObject to) throws Exception {
        txBegin(runtime);
        try {
            to.throwRuntimeException();
            txEnd(runtime);
        } catch (Exception e) {
            assertThat(TransactionalContext.isInTransaction()).isFalse();
        }
    }

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @CorfuTest
    public void useMapsAsMQs(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter(name = "map") SMRMap<Long, Long> testMap1,
        @Parameter(NUM_ITERATIONS_LOW) int numIterations,
        @Parameter(TIMEOUT_LONG) Duration timeout) throws Exception {

        final int nThreads = 4;
        CountDownLatch barrier = new CountDownLatch(nThreads-1);
        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);


        // 1st thread: producer of new "trigger" values
        scheduler.schedule(1, t -> {

            // wait for other threads to start
            assertThatCode(barrier::await)
                .doesNotThrowAnyException();
            log.debug("all started");

            for (int i = 0; i < numIterations; i++) {
                // place a value in the map
                log.debug("- sending 1st trigger " + i);
                testMap1.put(1L, (long) i);

                // await for the consumer condition to circulate back
                assertThatCode(s2::acquire)
                    .doesNotThrowAnyException();

                log.debug("- s2.await() finished at " + i);
            }
        });

        // 2nd thread: monitor map and wait for "trigger" values to show up, produce 1st signal
        scheduler.schedule(1, t -> {

            // signal start
            barrier.countDown();

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                while (testMap1.get(1L) == null || testMap1.get(1L) != (long) i) {
                    log.debug( "- wait for 1st trigger " + i);
                    assertThatCode(() -> TimeUnit.MILLISECONDS.sleep(busyDelay))
                        .doesNotThrowAnyException();
                }
                log.debug( "- received 1st trigger " + i);

                // 1st producer signal through lock
                // 1st producer signal
                log.debug( "- sending 1st signal " + i);
                s1.release();
            }
        });

        // 3rd thread: monitor 1st producer condition and produce a second "trigger"
        scheduler.schedule(1, t -> {

            // signal start
            barrier.countDown();

            for (int i = 0; i < numIterations; i++) {
                txBegin(runtime);
                log.debug( "- received 1st condition POST-lock PRE-await" + i);

                // wait for 1st producer signal
                assertThatCode(s1::acquire)
                    .doesNotThrowAnyException();
                log.debug( "- received 1st condition " + i);

                // produce another tigger value
                log.debug( "- sending 2nd trigger " + i);
                testMap1.put(2L, (long) i);
                txEnd(runtime);
            }
        });

        // 4th thread: monitor map and wait for 2nd "trigger" values to show up, produce second signal
        scheduler.schedule(1, t -> {

            // signal start
            barrier.countDown();

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                while (testMap1.get(2L) == null || testMap1.get(2L) != (long) i)
                    assertThatCode(() -> TimeUnit.MILLISECONDS.sleep(busyDelay))
                        .doesNotThrowAnyException();
                log.debug( "- received 2nd trigger " + i);

                // 2nd producer signal through lock
                // 2nd producer signal
                log.debug( "- sending 2nd signal " + i);
                s2.release();
            }
        });

        scheduler.execute(nThreads, timeout);
    }
}
