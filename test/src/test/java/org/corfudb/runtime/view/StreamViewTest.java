package org.corfudb.runtime.view;

import java.time.Duration;
import lombok.Getter;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.Parameter;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.corfudb.test.parameters.Param.CONCURRENCY_SOME;
import static org.corfudb.test.parameters.Param.NUM_ITERATIONS_LOW;
import static org.corfudb.test.parameters.Param.TIMEOUT_NORMAL;

/**
 * Created by mwei on 1/8/16.
 */
@CorfuTest
public class StreamViewTest {

    @CorfuTest
    public void canReadWriteFromStream(CorfuRuntime r)
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    /**
     * Test that a client can call IStreamView.remainingUpTo after a prefix trim.
     * If remainingUpTo contains trimmed addresses, then they are ignored.
     */
    @CorfuTest
    public void testRemainingUpToWithTrim(CorfuRuntime runtime, CorfuRuntime runtime2) {
        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(true)
                .build();

        IStreamView txStream = runtime.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        final int firstIter = 50;
        for (int x = 0; x < firstIter; x++) {
            byte[] data = "Hello World!".getBytes();
            txStream.append(data);
        }

        List<ILogData> entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(firstIter / 2);

        runtime.getAddressSpaceView().prefixTrim((firstIter - 1) / 2);
        runtime.getAddressSpaceView().invalidateServerCaches();
        runtime.getAddressSpaceView().invalidateClientCache();

        entries = txStream.remainingUpTo((firstIter - 1) / 2);
        assertThat(entries.size()).isEqualTo(0);

        entries = txStream.remainingUpTo(firstIter);
        assertThat(entries.size()).isEqualTo((firstIter / 2));

        // Open the stream with a new client
        txStream = runtime2.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID, options);
        entries = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(entries.size()).isEqualTo((firstIter / 2));
    }

    @CorfuTest
    public void canReadWriteFromStreamConcurrent(CorfuRuntime r,
        ConcurrentScheduler scheduler,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(TIMEOUT_NORMAL) Duration timeout)
            throws Exception {
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduler.schedule(iterations,
                i -> sv.append(testPayload));
        scheduler.execute(concurrency, timeout);

        scheduler.schedule(iterations,
                i -> assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes()));
        scheduler.execute(concurrency, timeout);
        assertThat(sv.next())
                .isEqualTo(null);
    }

    @CorfuTest
    public void canReadWriteFromStreamWithoutBackpointers(CorfuRuntime r,
        ConcurrentScheduler scheduler,
        @Parameter(CONCURRENCY_SOME) int concurrency,
        @Parameter(NUM_ITERATIONS_LOW) int iterations,
        @Parameter(TIMEOUT_NORMAL) Duration timeout)
            throws Exception {
        r.setBackpointersDisabled(true);
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        scheduler.schedule(iterations, i ->
                sv.append(testPayload));
        scheduler.execute(concurrency, timeout);

        scheduler.schedule(iterations, i ->
                assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes()));
        scheduler.execute(concurrency, timeout);
        assertThat(sv.next())
                .isEqualTo(null);
    }


    @CorfuTest
    public void canReadWriteFromCachedStream(CorfuRuntime r)
            throws Exception {
        r.setCacheDisabled(false);
        UUID streamA = UUID.nameUUIDFromBytes("stream A".getBytes());
        byte[] testPayload = "hello world".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    @CorfuTest
    public void canSeekOnStream(CorfuRuntime r)
        throws Exception
    {
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Try reading two entries
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        // Seeking to the beginning
        sv.seek(0);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("a".getBytes());

        // Seeking to the end
        sv.seek(2);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("c".getBytes());

        // Seeking to the middle
        sv.seek(1);
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @CorfuTest
    public void canFindInStream(CorfuRuntime r)
            throws Exception
    {
        IStreamView svA = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));
        IStreamView svB = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  B"));

        // Append some entries
        final long A_GLOBAL = 0;
        svA.append("a".getBytes());
        final long B_GLOBAL = 1;
        svB.append("b".getBytes());
        final long C_GLOBAL = 2;
        svA.append("c".getBytes());
        final long D_GLOBAL = 3;
        svB.append("d".getBytes());
        final long E_GLOBAL = 4;
        svA.append("e".getBytes());

        // See if we can find entries:
        // Should find entry "c"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(C_GLOBAL);
        // Should find entry "a"
        assertThat(svA.find(B_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(A_GLOBAL);
        // Should find entry "e"
        assertThat(svA.find(E_GLOBAL,
                IStreamView.SearchDirection.FORWARD_INCLUSIVE))
                .isEqualTo(E_GLOBAL);
        // Should find entry "c"
        assertThat(svA.find(C_GLOBAL,
                IStreamView.SearchDirection.REVERSE_INCLUSIVE))
                .isEqualTo(C_GLOBAL);

        // From existing to existing:
        // Should find entry "b"
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.REVERSE))
                .isEqualTo(B_GLOBAL);
        // Should find entry "d"
        assertThat(svB.find(B_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(D_GLOBAL);

        // Bounds:
        assertThat(svB.find(D_GLOBAL,
                IStreamView.SearchDirection.FORWARD))
                .isEqualTo(Address.NOT_FOUND);
    }

    @CorfuTest
    public void canDoPreviousOnStream(CorfuRuntime r)
            throws Exception
    {
        IStreamView sv = r.getStreamsView().get(
                CorfuRuntime.getStreamID("stream  A"));

        // Append some entries
        sv.append("a".getBytes());
        sv.append("b".getBytes());
        sv.append("c".getBytes());

        // Move backward should return null
        assertThat(sv.previous())
                .isNull();

        // Move forward
        sv.next(); // "a"
        sv.next(); // "b"

        // Should be now "a"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("a".getBytes());

        // Move forward, should be now "b"
        assertThat(sv.next().getPayload(r))
                .isEqualTo("b".getBytes());

        sv.next(); // "c"
        sv.next(); // null

        // Should be now "b"
        assertThat(sv.previous().getPayload(r))
                .isEqualTo("b".getBytes());
    }

    @CorfuTest
    public void streamCanSurviveOverwriteException(CorfuRuntime r)
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(0L);

        // Write to the stream, and read back. The hole should be filled.
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }

    @CorfuTest
    public void streamWillHoleFill(CorfuRuntime r)
            throws Exception {
        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Generate a hole.
        r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // Write to the stream, and read back. The hole should be filled.
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo("hello world".getBytes());

        assertThat(sv.next())
                .isEqualTo(null);
    }


    @CorfuTest
    public void streamWithHoleFill(CorfuRuntime r)
            throws Exception {
        UUID streamA = CorfuRuntime.getStreamID("stream A");

        byte[] testPayload = "hello world".getBytes();
        byte[] testPayload2 = "hello world2".getBytes();

        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        //generate a stream hole
        TokenResponse tr =
                r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        tr = r.getSequencerView().nextToken(Collections.singleton(streamA), 1);

        // read from an address that hasn't been written to
        // causing a hole fill
        r.getAddressSpaceView().read(tr.getToken().getTokenValue());


        sv.append(testPayload2);

        //make sure we can still read the stream.
        assertThat(sv.next().getPayload(r))
                .isEqualTo(testPayload);

        assertThat(sv.next().getPayload(r))
                .isEqualTo(testPayload2);
    }

    @CorfuTest
    public void prefixTrimThrowsException(CorfuRuntime r)
            throws Exception {
        //begin tests
        UUID streamA = CorfuRuntime.getStreamID("stream A");
        byte[] testPayload = "hello world".getBytes();

        // Write to the stream
        IStreamView sv = r.getStreamsView().get(streamA);
        sv.append(testPayload);

        // Trim the entry
        r.getAddressSpaceView().prefixTrim(0);
        r.getAddressSpaceView().gc();
        r.getAddressSpaceView().invalidateServerCaches();
        r.getAddressSpaceView().invalidateClientCache();

        // We should get a prefix trim exception when we try to read
        assertThatThrownBy(() -> sv.next())
                .isInstanceOf(TrimmedException.class);
    }
}
