package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import java.time.Duration;
import lombok.Getter;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.test.parameters.Param.CONCURRENCY_SOME;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LOW;
import static org.corfudb.test.parameters.Param.TIMEOUT_LONG;


/**
 * Created by mwei on 4/7/16.
 */
@CorfuTest
public class PutIfAbsentMapTest {


    @CorfuTest
    void putIfAbsentTest(CorfuRuntime runtime) {
        PutIfAbsentMap<String, String> stringMap =
            runtime.getObjectsView().build()
                .setStreamName("stringMap")
                .setTypeToken(new TypeToken<PutIfAbsentMap<String, String>>() {})
                .open();

        stringMap.put("a", "b");

        assertThat(stringMap.get("a"))
                .isEqualTo("b");

        assertThat(stringMap.putIfAbsent("a", "c"))
                .isFalse();

        assertThat(stringMap.get("a"))
                .isEqualTo("b");
    }

    @CorfuTest
    void putIfAbsentTestConcurrent(CorfuRuntime runtime,
                                   ConcurrentScheduler scheduler,
                                   @Parameter(NUM_ITERATIONS_LOW) int iterations,
                                   @Parameter(CONCURRENCY_SOME) int concurrency,
                                   @Parameter(TIMEOUT_LONG) Duration timeout)
            throws Exception {
        PutIfAbsentMap<String, Integer> stringMap =
            runtime.getObjectsView().build()
                .setStreamName("stringMap")
                .setTypeToken(new TypeToken<PutIfAbsentMap<String, Integer>>() {})
                .open();

        ConcurrentLinkedQueue<Boolean> resultList = new ConcurrentLinkedQueue<>();
        scheduler.schedule(iterations, x -> resultList.add(stringMap.putIfAbsent("a", x)));
        scheduler.execute(concurrency, timeout);

        long trueCount = resultList.stream()
                .filter(x -> x)
                .count();

        assertThat(trueCount)
                .isEqualTo(1);
    }

    @CorfuObject
    public static class PutIfAbsentMap<K, V> {

        HashMap<K, V> map = new HashMap<>();

        @MutatorAccessor(name="put")
        public V put(K key, V value) {
            return map.put(key, value);
        }

        @Accessor
        public V get(K key) {
            return map.get(key);
        }

        @MutatorAccessor(name="putIfAbsent")
        public boolean putIfAbsent(K key, V value) {
            if (map.get(key) == null) {
                map.put(key, value);
                return true;
            }
            return false;
        }

    }
}
