package org.corfudb.runtime;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * TODO: add javadoc
 *
 * Created by WenbinZhu on 11/20/18.
 */
@Slf4j
public class FetchLayoutUtil {

    /**
     * Fetches the updated layout from quorum of layout servers.
     *
     * @return quorum agreed layout.
     * @throws QuorumUnreachableException If unable to receive consensus on layout.
     */
    public static Layout fetchQuorumLayout(CompletableFuture<Layout>[] completableFutures) {

        QuorumFuturesFactory.CompositeFuture<Layout> quorumFuture = QuorumFuturesFactory
                .getQuorumFuture(Comparator.comparing(Layout::asJSONString), completableFutures);
        try {
            return quorumFuture.get();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) ee.getCause();
            }

            int reachableServers = (int) Arrays.stream(completableFutures)
                    .filter(cf -> !cf.isCompletedExceptionally())
                    .count();
            throw new QuorumUnreachableException(reachableServers, completableFutures.length);
        } catch (InterruptedException ie) {
            log.error("fetchQuorumLayout: Interrupted Exception.");
            Thread.currentThread().interrupt();
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }
}
