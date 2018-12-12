package org.corfudb.infrastructure.log;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.util.MetricsUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamLogCompactionTest extends AbstractCorfuTest {

    private static MetricRegistry metricRegistry;

    @BeforeClass
    public static void setUpMetrics() {
        metricRegistry = ServerContext.getMetrics();
        if (!MetricsUtils.isMetricsCollectionEnabled()) {
            MetricsUtils.metricsReportingSetup(metricRegistry);
        }
    }

    /**
     * Test that task catches all possible exceptions and doesn't break scheduled executor
     *
     * @throws InterruptedException thread sleep
     */
    @Test
    public void testCompaction() throws InterruptedException {
        log.debug("Start log compaction test");

        final int timeout = 10;
        final int initialDelay = 10;
        final int period = 10;

        StreamLog streamLog = mock(StreamLog.class);
        doThrow(new RuntimeException("err")).when(streamLog).compact();

        final long initialCompactionCounter = getCompactionCounter();
        StreamLogCompaction compaction = new StreamLogCompaction(streamLog,
                                                                 initialDelay,
                                                                 period,
                                                                 TimeUnit.MILLISECONDS,
                                                                 PARAMETERS.TIMEOUT_VERY_SHORT);

        final long expectedCompactionCounter = 2 + initialCompactionCounter;
        while(getCompactionCounter() < expectedCompactionCounter){
            TimeUnit.MILLISECONDS.sleep(timeout);
        }

        compaction.shutdown();

        assertThat(getCompactionCounter()).isGreaterThanOrEqualTo(expectedCompactionCounter);
    }

    private long getCompactionCounter() {
        return metricRegistry
                .timer(StreamLogCompaction.STREAM_COMPACT_METRIC)
                .getCount();
    }
}