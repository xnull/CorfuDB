package org.corfudb.runtime.view;

import com.google.common.reflect.TypeToken;
import java.time.Duration;
import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.concurrent.ConcurrentScheduler;
import org.corfudb.test.parameters.CorfuObjectParameter;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 2/18/16.
 */
@CorfuTest
public class ObjectsViewTest {

    public static boolean referenceTX(Map<String, String> smrMap) {
        smrMap.put("a", "b");
        assertThat(smrMap)
                .containsEntry("a", "b");
        return true;
    }

    @CorfuTest
    public void canCopyObject(CorfuRuntime r)
            throws Exception {
        Map<String, String> smrMap = r.getObjectsView().build()
                                                    .setStreamName("map a")
                                                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                                                    .open();

        smrMap.put("a", "a");

        Map<String, String> smrMapCopy = r.getObjectsView()
                .copy(smrMap, "map a copy");
        smrMapCopy.put("b", "b");

        assertThat(smrMapCopy)
                .containsEntry("a", "a")
                .containsEntry("b", "b");

        assertThat(smrMap)
                .containsEntry("a", "a")
                .doesNotContainEntry("b", "b");
    }

    @CorfuTest
    public void cannotCopyNonCorfuObject(CorfuRuntime r)
            throws Exception {
        assertThatThrownBy(() -> {
            r.getObjectsView().copy(new HashMap<String, String>(), CorfuRuntime.getStreamID("test"));
        }).isInstanceOf(RuntimeException.class);
    }

    @CorfuTest
    public void canAbortNoTransaction(CorfuRuntime r)
            throws Exception {
        r.getObjectsView().TXAbort();
    }

    @CorfuTest
    public void abortedTransactionDoesNotConflict(CorfuRuntime r,
        ConcurrentScheduler scheduler,
        @CorfuObjectParameter(stream="map a") SMRMap<String, String> map,
        @CorfuObjectParameter(stream="map a", options = {ObjectOpenOptions.NO_CACHE})
            SMRMap<String, String> mapCopy,
        @Parameter(Param.TIMEOUT_LONG) Duration timeout)
            throws Exception {
        final String mapA = "map a";
        //Enbale transaction logging
        r.setTransactionLogging(true);

        // TODO: fix so this does not require mapCopy.

        map.put("initial", "value");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);

        // Schedule two threads, the first starts a transaction and reads,
        // then waits for the second thread to finish.
        // the second starts a transaction, waits for the first tx to read
        // and commits.
        // The first thread then resumes and attempts to commit. It should abort.
        scheduler.schedule(1, t -> {
            assertThatThrownBy(() -> {
                r.getObjectsView().TXBegin();
                map.get("k");
                s1.release();   // Let thread 2 start.
                s2.acquire();   // Wait for thread 2 to commit.
                map.put("k", "v1");
                r.getObjectsView().TXEnd();
            }).isInstanceOf(TransactionAbortedException.class);
        });

        scheduler.schedule(1, t -> {
            assertThatCode(s1::acquire)
                    .doesNotThrowAnyException();// Wait for thread 1 to read
            r.getObjectsView().TXBegin();
            mapCopy.put("k", "v2");
            r.getObjectsView().TXEnd();
            s2.release();
        });

        scheduler.execute(2, timeout);

        // The result should contain T2s modification.
        assertThat(map)
                .containsEntry("k", "v2");

        IStreamView txStream = r.getStreamsView().get(ObjectsView
                .TRANSACTION_STREAM_ID);
        List<ILogData> txns = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(txns).hasSize(1);
        assertThat(txns.get(0).getLogEntry(r).getType())
                .isEqualTo(LogEntry.LogEntryType.MULTIOBJSMR);

        MultiObjectSMREntry tx1 = (MultiObjectSMREntry)txns.get(0).getLogEntry
                (r);
        MultiSMREntry entryMap = tx1.getEntryMap().get(CorfuRuntime.getStreamID(mapA));
        assertThat(entryMap).isNotNull();

        assertThat(entryMap.getUpdates().size()).isEqualTo(1);

        SMREntry smrEntry = entryMap.getUpdates().get(0);
        Object[] args = smrEntry.getSMRArguments();
        assertThat(smrEntry.getSMRMethod()).isEqualTo("put");
        assertThat((String) args[0]).isEqualTo("k");
        assertThat((String) args[1]).isEqualTo("v2");
    }

    @CorfuTest
    public void unrelatedStreamDoesNotConflict(CorfuRuntime r,
        @CorfuObjectParameter(stream="map a") SMRMap<String, String> smrMap)
            throws Exception {

        IStreamView streamB = r.getStreamsView().get(CorfuRuntime.getStreamID("b"));
        smrMap.put("a", "b");
        streamB.append(new SMREntry("hi", new Object[]{"hello"}, Serializers.PRIMITIVE));

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

    @CorfuTest
    public void unrelatedTransactionDoesNotConflict(CorfuRuntime r,
        @CorfuObjectParameter(stream="map a") SMRMap<String, String> smrMap,
        @CorfuObjectParameter(stream="map b") SMRMap<String, String> smrMapB)
            throws Exception {
        smrMap.put("a", "b");

        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMapB.put("b", b);
        r.getObjectsView().TXEnd();

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

}
