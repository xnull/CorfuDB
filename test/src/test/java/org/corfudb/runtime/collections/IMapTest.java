package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.test.parameters.Param.CONCURRENCY_SOME;
import static org.corfudb.test.parameters.Param.CONCURRENCY_TWO;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LARGE;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LOW;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_MODERATE;
import static org.corfudb.test.parameters.Param.TIMEOUT_LONG;
import static org.corfudb.test.parameters.Param.TIMEOUT_NORMAL;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import lombok.Data;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.concurrent.ConcurrentStateMachine;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.Parameter;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

@SuppressWarnings("unchecked")
public interface IMapTest<T extends Map> {

    @CorfuTest
    default void canReadWriteToSingle(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {
        testMap.clear();
        assertThat(testMap.put("a", "a"))
            .isNull();
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
    }

    @CorfuTest
    default void canReadWriteToSinglePrimitive(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {

        final double PRIMITIVE_1 = 2.4;
        final double PRIMITIVE_2 = 4.5;

        testMap.clear();
        assertThat(testMap.put(1L, PRIMITIVE_1))
            .isNull();
        assertThat(testMap.put(1L, PRIMITIVE_2))
            .isEqualTo(PRIMITIVE_1);
        assertThat(testMap.get(1L))
            .isEqualTo(PRIMITIVE_2);
    }

    @CorfuTest
    default void canGetId(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {
        UUID id = CorfuRuntime.getStreamID("test");

        ICorfuSMR testSMR = (ICorfuSMR) testMap;

        assertThat(id)
            .isEqualTo(testSMR.getCorfuStreamID());
    }


    @CorfuTest
    default void loadsFollowedByGets(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations
    )
        throws Exception {

        testMap.clear();

        for (int i = 0; i < iterations; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                .isNull();
        }

        for (int i = 0; i < iterations; i++) {
            assertThat(testMap.get(Integer.toString(i)))
                .isEqualTo(Integer.toString(i));
        }
    }

    @CorfuTest
    default void canContainOtherCorfuObjects(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "test 1") T testMap,
        @CorfuObjectParameter(stream = "test 2") T testMap2,
        @CorfuObjectParameter(stream = "test 1") T testMap3)
        throws Exception {
        testMap.put("z", "e");
        testMap2.put("a", testMap);

        assertThat(((T)testMap2.get("a")).get("z"))
            .isEqualTo("e");

        ((T)testMap2.get("a")).put("y", "f");

        assertThat(testMap.get("y"))
            .isEqualTo("f");

        assertThat(testMap3.get("y"))
            .isEqualTo("f");
    }

    @CorfuTest
    default void canContainNullObjects(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @CorfuObjectParameter T testMap2)
        throws Exception {

        testMap.clear();
        testMap.put("z", null);
        assertThat(testMap.get("z"))
            .isEqualTo(null);
        assertThat(testMap2.get("z"))
            .isEqualTo(null);
    }

    @CorfuTest
    default void loadsFollowedByGetsConcurrent(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int numRecords,
        @Parameter(CONCURRENCY_SOME) int numThreads,
        @Parameter(TIMEOUT_LONG) Duration timeout
    ) throws Exception {

        testMap.clear();

        scheduler.schedule(numThreads, threadNumber -> {
            int base = threadNumber * numRecords;
            for (int i = base; i < base + numRecords; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        scheduler.execute(numThreads, timeout);
        //calculateRequestsPerSecond("WPS", numRecords * numThreads, startTime);

        scheduler.schedule(numThreads, threadNumber -> {
            int base = threadNumber * numRecords;
            for (int i = base; i < base + numRecords; i++) {
                assertThat(testMap.get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        scheduler.execute(numThreads, timeout);
        //calculateRequestsPerSecond("RPS", numRecords * numThreads, startTime);
    }


    @CorfuTest
    @SuppressWarnings("unchecked")
    default void loadsFollowedByGetsConcurrentMultiView(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_MODERATE) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
        throws Exception {
        runtime.setBackpointersDisabled(true);
        // Increasing hole fill delay to avoid intermittent AppendExceptions.
        final int longHoleFillRetryLimit = 50;
        runtime.getParameters().setHoleFillRetry(longHoleFillRetryLimit);

        Map<String, String>[] testMap =
            IntStream.range(0, concurrency)
                .mapToObj(i ->
                    runtime.getObjectsView()
                        .build()
                        .setStreamID(UUID.randomUUID())
                        .setTypeToken(new TypeToken<T>(getClass()) {
                        })
                        .addOption(ObjectOpenOptions.NO_CACHE)
                        .open())
                .toArray(Map[]::new);

        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                assertThat(testMap[threadNumber].put(Integer.toString(i), Integer.toString(i)))
                    .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        scheduler.execute(concurrency, timeout);
        //calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                assertThat(testMap[threadNumber].get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        scheduler.execute(concurrency, timeout);
        //calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }


    @CorfuTest
    @SuppressWarnings("unchecked")
    default void collectionsStreamInterface(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {

        testMap.put("a", "b");
        runtime.getObjectsView().TXBegin();
        if (testMap.values().stream().anyMatch(x -> x.equals("c"))) {
            throw new Exception("test");
        }
        testMap.compute("b",
            (k, v) -> "c");
        runtime.getObjectsView().TXEnd();
        assertThat(testMap)
            .containsEntry("b", "c");
    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    default void readSetDiffFromWriteSet(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter(stream="test1") T testMap,
        @CorfuObjectParameter(stream="test2") T testMap2,
        @Parameter(CONCURRENCY_TWO) int concurrency,
        @Parameter(TIMEOUT_NORMAL) Duration timeout)
        throws Exception {

        testMap.put("a", "b");
        testMap2.put("a", "c");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);
        scheduler.schedule(1, threadNumber -> {
            assertThatCode(() -> s1.tryAcquire(timeout.toMillis(),
                TimeUnit.MILLISECONDS))
                .doesNotThrowAnyException();
            runtime.getObjectsView().TXBegin();
            testMap2.put("a", "d");
            runtime.getObjectsView().TXEnd();
            s2.release();
        });

        scheduler.schedule(1, threadNumber -> {
            runtime.getObjectsView().TXBegin();
            testMap.compute("b", (k, v) -> testMap2.get("a"));
            s1.release();
            assertThatCode(() -> s2.tryAcquire(timeout.toMillis(),
                TimeUnit.MILLISECONDS))
                .doesNotThrowAnyException();
            assertThatThrownBy(() -> runtime.getObjectsView().TXEnd())
                .isInstanceOf(TransactionAbortedException.class);
        });

        scheduler.execute(concurrency, timeout);
    }

    @CorfuTest
    default void canUpdateSingleObjectTransacationally(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {

        runtime.getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
            .isNull();
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
        runtime.getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
            .isEqualTo("b");
    }

    @CorfuTest
    default void multipleTXesAreApplied(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations)
        throws Exception {


        IntStream.range(0, iterations).asLongStream()

            .forEach(l -> {
                try {
                    assertThat(testMap)
                        .hasSize((int) l);
                    runtime.getObjectsView().TXBegin();
                    assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                        .isNull();
                    assertThat(testMap)
                        .hasSize((int) l + 1);
                    runtime.getObjectsView().TXEnd();
                    assertThat(testMap)
                        .hasSize((int) l + 1);
                } catch (TransactionAbortedException tae) {
                    throw new RuntimeException(tae);
                }
            });

        assertThat(testMap)
            .hasSize(iterations);
    }

    @CorfuTest
    default void multipleTXesAreAppliedWOAccessors(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations)
        throws Exception {

        IntStream.range(0, iterations).asLongStream()
            .forEach(l -> {
                try {
                    runtime.getObjectsView().TXBegin();
                    assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                        .isNull();
                    runtime.getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    throw new RuntimeException(tae);
                }
            });

        assertThat(testMap)
            .hasSize(iterations);
    }


    @CorfuTest
    @SuppressWarnings("unchecked")
    default void mutatorFollowedByATransaction(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {

        testMap.clear();
        runtime.getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
            .isNull();
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
        runtime.getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
            .isEqualTo("b");
    }

    @CorfuTest
    default void objectViewCorrectlyReportsInsideTX(CorfuRuntime runtime)
        throws Exception {
        assertThat(runtime.getObjectsView().TXActive())
            .isFalse();
        runtime.getObjectsView().TXBegin();
        assertThat(runtime.getObjectsView().TXActive())
            .isTrue();
        runtime.getObjectsView().TXEnd();
        assertThat(runtime.getObjectsView().TXActive())
            .isFalse();
    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    default void canUpdateSingleObjectTransacationallyWhenCached(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {
        runtime.setCacheDisabled(false);
        runtime.getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
            .isNull();
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
        runtime.getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
            .isEqualTo("b");
    }


    @CorfuTest
    default void abortedTransactionsCannotBeReadOnSingleObject(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {

        testMap.clear();
        testMap.put("z", "z");
        assertThat(testMap.size())
            .isEqualTo(1);


        runtime.getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
            .isNull();
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
        testMap.clear();
        runtime.getObjectsView().TXAbort();
        assertThat(testMap.size())
            .isEqualTo(1);
    }

    @CorfuTest
    default void modificationDuringTransactionCausesAbort(CorfuRuntime runtime,
        @CorfuObjectParameter(stream = "A") T testMap)
        throws Exception {

        assertThat(testMap.put("a", "z"));
        runtime.getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
            .isEqualTo("z");
        assertThat(testMap.put("a", "b"))
            .isEqualTo("a");
        assertThat(testMap.get("a"))
            .isEqualTo("b");
        CompletableFuture cf = CompletableFuture.runAsync(() -> {
            Map<String, String> testMap2 = runtime.getObjectsView()
                .build()
                .setStreamName("A")
                .setSerializer(Serializers.JSON)
                .addOption(ObjectOpenOptions.NO_CACHE)
                .setTypeToken(new TypeToken<T>(getClass()) {})
                .open();

            runtime.getObjectsView().TXBegin();
            testMap2.put("a", "f");
            runtime.getObjectsView().TXEnd();
        });
        cf.join();
        assertThatThrownBy(() -> runtime.getObjectsView().TXEnd())
            .isInstanceOf(TransactionAbortedException.class);
    }

    @CorfuTest
    default void smrMapCanContainCustomObjects(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {
        testMap.put("A", new TestObject("A", 2, ImmutableMap.of("A", "B")));
        assertThat(((TestObject)testMap.get("A")).getTestString())
            .isEqualTo("A");
        assertThat(((TestObject)testMap.get("A")).getTestInt())
            .isEqualTo(2);
    }

    @CorfuTest
    default void smrMapCanContainCustomObjectsInsideTXes(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations)
        throws Exception {
        IntStream.range(0, iterations)
            .forEach(l -> {
                try {
                    runtime.getObjectsView().TXBegin();
                    testMap.put(Integer.toString(l),
                        new TestObject(Integer.toString(l), l,
                            ImmutableMap.of(
                                Integer.toString(l), l)));
                    if (l > 0) {
                        assertThat(((TestObject)testMap.get(Integer.toString(l - 1)))
                            .getTestInt())
                            .isEqualTo(l - 1);
                    }
                    runtime.getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    throw new RuntimeException(tae);
                }
            });

        assertThat(((TestObject)testMap.get("3")).getTestString())
            .isEqualTo("3");
        assertThat(((TestObject)testMap.get("3")).getTestInt())
            .isEqualTo(Integer.parseInt("3"));
    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    default void unusedMutatorAccessor(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap)
        throws Exception {
        testMap.put("a", "z");
    }


    default void getMultiViewSM(int numThreads, CorfuRuntime runtime,
        ConcurrentStateMachine stateMachine, AtomicInteger aborts) {

        UUID mapStream = UUID.randomUUID();
        Map<String, String>[] testMap =
            IntStream.range(0, numThreads)
                .mapToObj(i ->
                    runtime.getObjectsView()
                        .build()
                        .setStreamID(mapStream)
                        .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                        })
                        .addOption(ObjectOpenOptions.NO_CACHE)
                        .open())
                .toArray(Map[]::new);

        // # keys indicate how much contention there will be
        final int numKeys = numThreads * 5;

        Random r = new Random();

        // state 0: start a transaction
        stateMachine.addStep((ignored_task_num) -> {
            runtime.getObjectsView().TXBegin();
        });

        // state 1: do a put and a get
        stateMachine.addStep( (task_num) -> {
            final int putKey = r.nextInt(numKeys);
            final int getKey = r.nextInt(numKeys);
            testMap[task_num%numThreads].put(Integer.toString(putKey),
                testMap[task_num%numThreads].get(Integer.toString(getKey)));
        });

        // state 2 (final): ask to commit the transaction
        stateMachine.addStep( (ignored_task_num) -> {
            try {
                runtime.getObjectsView().TXEnd();
            } catch (TransactionAbortedException tae) {
                aborts.incrementAndGet();
            }
        });

    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    default void concurrentAbortMultiViewInterleaved(CorfuRuntime runtime,
        ConcurrentStateMachine stateMachine,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations)
        throws Exception {

        long startTime = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger();

        getMultiViewSM(concurrency, runtime, stateMachine, aborts);
        // invoke the interleaving engine
        stateMachine.executeInterleaved(concurrency, iterations * concurrency);

        // print stats..
        //calculateRequestsPerSecond("TPS", iterations * concurrency, startTime);
        //calculateAbortRate(aborts.get(), iterations * concurrency);
    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    default void concurrentAbortMultiViewThreaded(CorfuRuntime runtime,
        ConcurrentStateMachine stateMachine,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
        throws Exception {
        long startTime = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger();

        getMultiViewSM(concurrency, runtime, stateMachine, aborts);
        // invoke the interleaving engine
        stateMachine.executeThreaded(iterations * concurrency, concurrency, timeout);

        // print stats..
        //calculateRequestsPerSecond("TPS", iterations * concurrency, startTime);
        //calculateAbortRate(aborts.get(), iterations * concurrency);
    }

    @CorfuTest
    default void bulkReads(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @CorfuObjectParameter(options = {ObjectOpenOptions.NO_CACHE})
            T testMap2,
        @Parameter(NUM_ITERATIONS_LOW) int iterations)
        throws Exception {
        testMap.clear();
        for (int i = 0; i < iterations; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                .isNull();
        }

        // Do a bulk read of the stream by initializing a new view.
        final int num_threads = 1;

        // Do a get to prompt the sync
        assertThat(testMap2.get(Integer.toString(0)))
            .isEqualTo(Integer.toString(0));
        long endTime = System.nanoTime();

        final int MILLISECONDS_TO_MICROSECONDS = 1000;
        //testStatus += "Time to sync whole stream=" + String.format("%d us",
        //        (endTime - startTime) / MILLISECONDS_TO_MICROSECONDS);
    }


    @CorfuTest
    default void manipulateSets(CorfuRuntime runtime,
        @CorfuObjectParameter T testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations) {


        // populate the map
        for (long i = 0; i < (long) iterations; i++)
            testMap.put(i, i);

        assertThat(testMap.get(0L))
            .isEqualTo(0L);
        assertThat(testMap.size())
            .isEqualTo(iterations);

        // get a snapshot of the keys
        Set<Long> keys = testMap.keySet();
        assertThat(keys.size())
            .isEqualTo(iterations);
        assertThat(keys.contains(0L))
            .isTrue();

        // manipulate the map, verify that keys set is unmodified,
        // the original map is modified
        testMap.remove(0L);

        assertThat(keys.size())
            .isEqualTo(iterations);
        assertThat(keys.contains(0L))
            .isTrue();

        assertThat(testMap.containsKey(0L))
            .isFalse();
        assertThat(testMap.size())
            .isEqualTo(iterations-1);

    }

    @CorfuTest
    default void manipulateSetsConcurrent(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter T testMap,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
        throws Exception {
        CountDownLatch l1 = new CountDownLatch(1);
        CountDownLatch l2 = new CountDownLatch(1);
        CountDownLatch l3 = new CountDownLatch(1);

        // Block until sequencer operational.
        runtime.getSequencerView().nextToken(Collections.EMPTY_SET, 0);

        // first thread: create and manipulate map
        scheduler.schedule(1, t -> {

            // signal start to second thread
            l1.countDown();

            // populate the map
            for (long i = 0; i < (long) iterations; i++)
                testMap.put(i, i);

            assertThat(testMap.get(0L))
                .isEqualTo(0L);
            assertThat(testMap.size())
                .isEqualTo(iterations);

            // wait for second thread to advance
            assertThatCode(l2::await)
                .doesNotThrowAnyException();

            // manipulate the map, verify that keys set is unmodified,
            // the original map is modified
            testMap.remove(0L);

            assertThat(testMap.containsKey(0L))
                .isFalse();
            assertThat(testMap.size())
                .isEqualTo(iterations - 1);

            // allow third thread to proceed
            l3.countDown();
        });

        // 2nd thread: get an immutable copy of the keys in the
        // at an arbitrary snapshot
        scheduler.schedule(1, t -> {
            assertThatCode(l1::await)
                .doesNotThrowAnyException();

            // get a snapshot of the keys;
            // we don't know at which point the snapshot will be,
            // relative to the other thread
            Set<Long> keys = testMap.keySet();
            int snapshotSize = keys.size();

            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                    .isTrue();

            // signal that one snapshot was taken already
            l2.countDown();

            // verify that the immutable snapshot remains immutable
            while (l3.getCount() > 0) {
                assertThat(keys.size())
                    .isEqualTo(snapshotSize);
                if (snapshotSize > 0)
                    assertThat(keys.contains(0L))
                        .isTrue();
            }
            assertThatCode(l3::await)
                .doesNotThrowAnyException();
            assertThat(keys.size())
                .isEqualTo(snapshotSize);
            if (snapshotSize > 0)
                assertThat(keys.contains(0L))
                    .isTrue();
        } );

        scheduler.schedule(1, t -> {
            assertThatCode(l3::await)
                .doesNotThrowAnyException();

            // get a snapshot of the keys;
            Set<Long> keys = testMap.keySet();
            assertThat(keys.size())
                .isEqualTo(iterations-1);
            assertThat(keys.contains(0L))
                .isFalse();
        });

        scheduler.execute(concurrency, timeout);
    }

    @Data
    @ToString
    class TestObject {
        final String testString;
        final int testInt;
        final Map<String, Object> deepMap;
    }

}
