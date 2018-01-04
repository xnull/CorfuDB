package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.test.parameters.Param.CONCURRENCY_SOME;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LOW;
import static org.corfudb.test.parameters.Param.TIMEOUT_LONG;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.Parameter;

@CorfuTest
public class FGMapTest implements IMapTest<FGMap> {

    @CorfuTest
    void checkClearCalls(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
    {
        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                testMap.clear();
                assertThat(testMap)
                    .isEmpty();
            }
        });

        long startTime = System.currentTimeMillis();
        scheduler.execute(concurrency, timeout);
        //calculateRequestsPerSecond("OPS", iterations * concurrency, startTime);
    }



    @CorfuTest
    void abortRateIsLowForSimpleTX(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
        throws Exception {

        AtomicInteger aborts = new AtomicInteger();
        testMap.clear();

        scheduler.schedule(concurrency, threadNumber -> {
            int base = threadNumber * iterations;
            for (int i = base; i < base + iterations; i++) {
                try {
                    runtime.getObjectsView().TXBegin();
                    assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
                    runtime.getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    aborts.incrementAndGet();
                }
            }
        });

        long startTime = System.currentTimeMillis();
        scheduler.execute(concurrency, timeout);
        //calculateRequestsPerSecond("TPS", iterations * concurrency, startTime);
        //calculateAbortRate(aborts.get(), iterations * concurrency);
    }


    @CorfuTest
    void sizeIsCorrect(CorfuRuntime runtime,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations) {
        testMap.clear();
        assertThat(testMap)
            .isEmpty();

        for (int i = 0; i < iterations; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
            .hasSize(iterations);
    }

    @CorfuTest
    void canNestTX(CorfuRuntime runtime,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations) {
        testMap.clear();
        assertThat(testMap)
            .isEmpty();

        for (int i = 0; i < iterations; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        runtime.getObjectsView().TXBegin();
        int size = testMap.size();
        testMap.put("size", Integer.toString(size));
        runtime.getObjectsView().TXEnd();

        assertThat(testMap.size())
            .isEqualTo(iterations + 1);
        assertThat(testMap.get("size"))
            .isEqualTo(Integer.toString(iterations));
    }

    @CorfuTest
    public void canClear(CorfuRuntime runtime,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations) {
        testMap.clear();
        assertThat(testMap)
            .isEmpty();

        for (int i = 0; i < iterations; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }

        assertThat(testMap)
            .hasSize(iterations);

        testMap.put("a", "b");
        testMap.clear();
        assertThat(testMap)
            .isEmpty();
    }

    @CorfuTest
    void canClearInTX(CorfuRuntime runtime,
        @CorfuObjectParameter FGMap<String, String> testMap,
        @Parameter(NUM_ITERATIONS_LOW) int iterations) {
        testMap.clear();
        assertThat(testMap)
            .isEmpty();

        runtime.getObjectsView().TXBegin();
        for (int i = 0; i < iterations; i++) {
            testMap.put(Integer.toString(i), Integer.toString(i));
        }
        testMap.clear();
        assertThat(testMap)
            .isEmpty();
        runtime.getObjectsView().TXEnd();

        assertThat(testMap)
            .hasSize(0);
    }

    /** This test takes too long to run with a FG map, so we tune down the
     *  number of iterations.
     */
    @Override
    public void loadsFollowedByGetsConcurrentMultiView(CorfuRuntime runtime,
        ConcurrentScheduler scheduler,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_LONG) Duration timeout)
        throws Exception {
        IMapTest.super
            .loadsFollowedByGetsConcurrentMultiView(runtime, scheduler, concurrency,
                iterations, timeout);
    }
}
