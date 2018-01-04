package org.corfudb.infrastructure.log;

import java.time.Duration;
import java.util.function.IntConsumer;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.AbstractCorfuTest;
import org.corfudb.AbstractCorfuTest.CallableConsumer;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.concurrent.ConcurrentScheduler.SchedulerException;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by kspirov on 3/8/17.
 */
@Slf4j
@CorfuTest
public class MultiReadWriteLockTest {

    @CorfuTest
    public void testWriteAndReadLocksAreReentrant(ConcurrentScheduler scheduler,
        @Parameter(Param.CONCURRENCY_ONE) int concurrency,
        @Parameter(Param.TIMEOUT_NORMAL) Duration timeout) throws Exception {
        IntConsumer c = (r) -> {
            MultiReadWriteLock locks = new MultiReadWriteLock();
            try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l)) {
                try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
                    try (MultiReadWriteLock.AutoCloseableLock ignored3 = locks.acquireReadLock(1l)) {
                        try (MultiReadWriteLock.AutoCloseableLock ignored4 = locks.acquireReadLock(1l)) {
                        }
                    }
                }
            }
        };
        scheduler.schedule(concurrency, c);
        scheduler.execute(concurrency, timeout);
        // victory - we were not canceled
    }

    @CorfuTest
    public void testIndependentWriteLocksDoNotSynchronize(ConcurrentScheduler scheduler,
        @Parameter(Param.CONCURRENCY_SOME) int concurrency,
        @Parameter(Param.TIMEOUT_NORMAL) Duration timeout) throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(concurrency);
        // Here  test that the lock does not synchronize when the value is different.
        // Otherwise one of the locks would halt on await, and the others - when acquiring the write log.
        IntConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireWriteLock((long)r)){
                assertThatCode(entry::await)
                    .doesNotThrowAnyException();
            }
        };
        scheduler.schedule(concurrency, c);
        scheduler.execute(concurrency, timeout);
        // victory - we were not canceled
    }



    @CorfuTest
    public void testWriteLockSynchronizes(ConcurrentScheduler scheduler,
        @Parameter(Param.CONCURRENCY_SOME) int concurrency,
        @Parameter(Param.TIMEOUT_SHORT) Duration timeout) throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(concurrency);
        // All threads should block, nobody should exit
        IntConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireWriteLock(1l)) {
                assertThatCode(entry::await)
                    .doesNotThrowAnyException();
            }
        };
        scheduler.schedule(concurrency, c);
        try {
            scheduler.execute(concurrency, timeout);
            fail();
        } catch (SchedulerException e) {
            e.getExceptionMap().forEach((threadNum, exception) -> {
                assertThat(exception)
                    .isInstanceOf(CancellationException.class);
            });
        }
    }



    @CorfuTest
    public void testReadLock(ConcurrentScheduler scheduler,
        @Parameter(Param.CONCURRENCY_SOME) int concurrency,
        @Parameter(Param.TIMEOUT_NORMAL) Duration timeout) throws Exception {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        CyclicBarrier entry = new CyclicBarrier(concurrency);
        IntConsumer c = (r) -> {
            try(MultiReadWriteLock.AutoCloseableLock ignored = locks.acquireReadLock(1l)){
                assertThatCode(entry::await)
                    .doesNotThrowAnyException();
            }
        };
        scheduler.schedule(concurrency, c);
        scheduler.execute(concurrency, timeout);
    }



    @CorfuTest
    public void testWriteLockNotPermittedInReadLock()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(1l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @CorfuTest
    public void testIncorrectLockOrder()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @CorfuTest
    public void testCorrectLockOrder()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(1l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(2l)) {
            }
        }
        // no RuntimeException as expected
    }

    @CorfuTest
    public void testLockCorrectlyDeregistered()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
        }
        try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
        }
        // let's nest
        try (MultiReadWriteLock.AutoCloseableLock ignored0 = locks.acquireReadLock(0l)) {
            try (MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireReadLock(2l)) {
            }
            try (MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireWriteLock(1l)) {
            }
        }
        // no RuntimeException as expected
    }

    @CorfuTest
    public void testLockCloseIsIdempotent()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(2l);
        ignored1.close();
        ignored1.close();
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(1l);
        ignored2.close();
        ignored2.close();
    }

    @CorfuTest
    public void testWrongUnlocksOrderCatched()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l);
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(2l);
        try {
            ignored1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }

    @CorfuTest
    public void testWrongUnlocksTypeCatched()  {
        MultiReadWriteLock locks = new MultiReadWriteLock();
        MultiReadWriteLock.AutoCloseableLock ignored1 = locks.acquireWriteLock(1l);
        MultiReadWriteLock.AutoCloseableLock ignored2 = locks.acquireReadLock(1l);
        try {
            ignored1.close();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }



}
