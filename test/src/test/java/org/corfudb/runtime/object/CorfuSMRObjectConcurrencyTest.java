package org.corfudb.runtime.object;

import java.time.Duration;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dmalkhi on 12/4/16.
 */
@CorfuTest
public class CorfuSMRObjectConcurrencyTest {

    @CorfuTest
    public void testCorfuSharedCounterConcurrentReads(CorfuRuntime runtime,
            ConcurrentScheduler scheduler,
            @CorfuObjectParameter(stream = "test") CorfuSharedCounter sharedCounter,
            @Parameter(Param.CONCURRENCY_SOME) int concurrency,
            @Parameter(Param.NUM_ITERATIONS_LOW) int writerwork,
            @Parameter(Param.TIMEOUT_LONG) Duration timeout
        ) throws Exception {

        final int COUNTER_INITIAL = 55;

        sharedCounter.setValue(COUNTER_INITIAL);

        int readconcurrency =concurrency * 2;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduler.schedule(concurrency, t -> {
                    for (int i = 0; i < writerwork; i++)
                        sharedCounter.setValue(t*writerwork + i);
                }
        );

        scheduler.schedule(readconcurrency-concurrency, t -> {
                    int lastread = -1;
                    for (int i = 0; i < writerwork; i++) {
                        int res = sharedCounter.getValue();
                        boolean assertflag =
                                (
                                        ( ((lastread < writerwork && res < writerwork) || (lastread >= writerwork && res >= writerwork) ) && lastread <= res ) ||
                                                ( (lastread < writerwork && res >= writerwork) || (lastread >= writerwork && res < writerwork) )
                                );
                        assertThat(assertflag)
                                .isTrue();
                    }
                }
        );

        scheduler.execute(readconcurrency, timeout);

    }
}
