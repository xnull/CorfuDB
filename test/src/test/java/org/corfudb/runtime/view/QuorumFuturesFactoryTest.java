/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 */
package org.corfudb.runtime.view;

import java.time.Duration;
import org.apache.maven.wagon.ConnectionException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.Parameter;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.corfudb.test.parameters.Param.TIMEOUT_VERY_SHORT;
import static org.junit.Assert.*;

/**
 * Tests the futures used in the quorum replication
 * Created by Konstantin Spirov on 2/6/2017.
 */
@CorfuTest
public class QuorumFuturesFactoryTest {

    @CorfuTest
    public void testSingleFutureIncompleteComplete(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        Future<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1);
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        assertFalse(result.isDone());
        f1.complete("ok");
        Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        assertTrue(result.isDone());
    }

    @CorfuTest
    public void testInfiniteGet() throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        Future<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1);
        f1.complete("ok");
        Object value = result.get();
        assertEquals("ok", value);
        assertTrue(result.isDone());
    }


    @CorfuTest
    public void test2FuturesIncompleteComplete(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        Future<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2);
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f1.complete("ok");
        Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
    }

    @CorfuTest
    public void test3FuturesIncompleteComplete(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2, f3);
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f3.complete("ok");
        Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        f1.complete("ok");
        value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        assertFalse(result.isConflict());
    }

    @CorfuTest
    public void test3FuturesWithFirstWinnerIncompleteComplete(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        Future<String> result = QuorumFuturesFactory.getFirstWinsFuture(String::compareTo, f1, f2, f3);
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f2.complete("ok");
        Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
    }


    @CorfuTest
    public void testException(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2, f3);
        f1.completeExceptionally(new ConnectionException(""));
        f3.complete("");
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected behaviour
        }
        f2.completeExceptionally(new IllegalArgumentException());
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            // expected behaviour
        }
        assertTrue(result.isDone());
        Set<Class> set = new LinkedHashSet<>();
        for (Throwable t: result.getThrowables()) {
            set.add(t.getClass());
        }
        assertTrue(set.contains(ConnectionException.class));
        assertTrue(set.contains(IllegalArgumentException.class));
    }

    @CorfuTest
    public void testFailFastExceptionSingle(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo,
                new CompletableFuture[]{f1, f2, f3}, NullPointerException.class, IllegalAccessError.class);
        f1.completeExceptionally(new NullPointerException());
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            // expected behaviour
        }
        assertTrue(result.isDone());
        Set<Class> set = new LinkedHashSet<>();
        for (Throwable t: result.getThrowables()) {
            set.add(t.getClass());
        }
        assertTrue(set.contains(NullPointerException.class));
    }


    @CorfuTest
    public void testCanceledFromInside(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1);
        f1.cancel(true);
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof QuorumUnreachableException);
            assertEquals(0, ((QuorumUnreachableException)e.getCause()).getReachable());
            assertEquals(1, ((QuorumUnreachableException)e.getCause()).getRequired());
        }
        assertTrue(result.isCancelled());
        assertTrue(result.isDone());
        assertFalse(result.isConflict());
        assertTrue(result.getThrowables().isEmpty());
    }


    @CorfuTest
    public void testCanceledPlusException(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2);
        f1.cancel(true);
        f2.completeExceptionally(new NullPointerException());
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            // expected behaviour
        }
        assertTrue(result.isCancelled());
        assertTrue(result.isDone());
        assertEquals(result.getThrowables().iterator().next().getClass(), NullPointerException.class);
    }


    @CorfuTest
    public void test3FuturesCompleteWithResolvedConflict(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout
    ) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2, f3);
        f2.complete("ok");
        try {
            result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (TimeoutException e) {
            // expected
        }
        f3.complete("not-ok");
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            // expected
        }
        f1.complete("ok");
        Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        assertEquals("ok", value);
        assertTrue(result.isConflict());
    }


    @CorfuTest
    public void test3FuturesCompleteWithUnresolvedConflict(
        @Parameter(TIMEOUT_VERY_SHORT) Duration timeout) throws Exception {
        CompletableFuture<String> f1 = new CompletableFuture<>();
        CompletableFuture<String> f2 = new CompletableFuture<>();
        CompletableFuture<String> f3 = new CompletableFuture<>();
        QuorumFuturesFactory.CompositeFuture<String> result = QuorumFuturesFactory.getQuorumFuture(String::compareTo, f1, f2, f3);
        f2.complete("ok");
        f3.complete("not-ok");
        f1.complete("1/3 split brain");
        try {
            Object value = result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof QuorumUnreachableException);
            assertEquals(1, ((QuorumUnreachableException) e.getCause()).getReachable());
            assertEquals(2, ((QuorumUnreachableException) e.getCause()).getRequired());
        }
        assertTrue(result.isConflict());
    }

}
