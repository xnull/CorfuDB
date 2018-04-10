package org.corfudb.infrastructure.log;

import lombok.Data;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.carrotsearch.sizeof.RamUsageEstimator.sizeOf;

/**
 * Created by Maithem on 4/10/18.
 */
public class LogChecker {

    Map<UUID, Map<ByteBuffer, Long>> streamRegistry = new HashMap<>();

    long staleBytes = 0;


    public void genData() {
        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        SMRMap<String, String> map1 = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();
        SMRMap<String, String> map2 = rt.getObjectsView().build().setStreamName("s1").setType(SMRMap.class).open();

        for (int x = 0; x < 10; x++) {
            rt.getObjectsView().TXBegin();
            map1.put(String.valueOf(1), String.valueOf(x));
            //map2.put(String.valueOf(x), String.valueOf(x));
            rt.getObjectsView().TXEnd();
        }

    }

    @Test
    public void checkLog() {

        String logPathDir = "/home/maithem/perfEng/4-11/corfu";
        ServerContext sc = new ServerContextBuilder()
                .setLogPath(logPathDir)
                .setMemory(false)
                .build();

        String luDir = logPathDir + File.separator + "log";
        File path = new File(luDir);
        File[] files = path.listFiles();

        List<String> segmentFiles = new ArrayList<>();

        for (File segFile : files) {
            if (segFile.getAbsolutePath().endsWith(".log")) {
                segmentFiles.add(segFile.getAbsolutePath());
            }
        }

        Collections.sort(segmentFiles);
        String tailSegmentPath = segmentFiles.get(segmentFiles.size() - 1);

        String[] tmpList = tailSegmentPath.split(File.separator);

        // get the largest segment
        String tailSegSuffix = tmpList[tmpList.length - 1];

        // remove .log
        long tailSegment = Long.valueOf(tailSegSuffix.substring(0, tailSegSuffix.length() - 4));
        sc.setStartingAddress(0);
        sc.setTailSegment(tailSegment);

        StreamLogFiles log = new StreamLogFiles(sc, false);

        for (long segment = tailSegment; segment >= 0; segment--) {
            SegmentHandle sh = log.getSegmentHandleForAddress(segment * 10_000);
            List<Long> segmentAddresses = new ArrayList<>(sh.getKnownAddresses().keySet());
            Collections.sort(segmentAddresses);
            Collections.reverse(segmentAddresses);

            processSegment(segmentAddresses, log);
        }

        System.out.println("staleBytes: " + staleBytes);
        System.out.println("skipped: " + skipped);
        System.out.println("singleSMR: " + singleSMR);
        System.out.println("totalEntries: " + totalEntries);
        System.out.println("totalSMR: " + totalSMR);
        System.out.println("conflictSMR: " + conflictSMR);
        System.out.println("Size of ds: " + sizeOf(streamRegistry));
    }


    long currentAddress;
    LogData currentLd;

    void processSegment(List<Long> addresses, StreamLog log) {

        for (int x = 0; x < addresses.size(); x++) {

            currentAddress = addresses.get(x);
            currentLd = log.read(currentAddress);

            if (currentLd.isHole()) {
                skipped++;
                continue;
            } else if (currentLd.hasCheckpointMetadata() || currentLd.isTrimmed()) {
                skipped++;
                continue;
                //throw new IllegalStateException("can't be cp/trimmed");
            }

            byte[] data = currentLd.getData();
            ByteBuffer buf = ByteBuffer.wrap(data);
            processEntry(buf);
        }
    }

    long skipped = 0;
    long singleSMR = 0;
    long totalEntries = 0;
    long totalSMR = 0;
    long conflictSMR = 0;


    void processEntry(ByteBuffer buf) {

        totalEntries++;

        int pos = buf.position();

        byte magic = buf.get();

        if (magic != 0x42) {
            throw new IllegalStateException("Couldn't find magic");
        }

        byte type = buf.get();

        buf.position(pos);

        if (type == 7) {
            // multi smr object
            parseMultiObjSMR(buf);
        } else if (type == 1) {
            parseSMR(buf);
            singleSMR++;
        } else if (type == 8) {
            parseMultiSMR(buf);
        } else {
            throw new IllegalStateException("Unknown entry type " + type);
        }

        if (buf.hasRemaining()) {
            throw new IllegalStateException("shouldn't have extra bytes");
        }

    }

    void parseMultiObjSMR(ByteBuffer buf) {

        byte magic = buf.get();

        if (magic != 0x42) {
            throw new IllegalStateException("Couldn't find magic");
        }

        byte type = buf.get();

        if (type != 7) {
            // multi smr object
            throw new IllegalStateException("Unknown entry type " + type);
        }

        int numMultiObjs = buf.getInt();

        int staleMultiObjectSMR = 0;

        for (int x = 0; x < numMultiObjs; x++) {
            UUID id = new UUID(buf.getLong(), buf.getLong());
            // probably need to process this backwards ?
            List<SMRMeta> smrs = parseMultiSMR(buf);
            Collections.reverse(smrs);

            int staleStreamEntries = 0;
            for (SMRMeta smrMeta : smrs) {

                if (streamRegistry.get(id) == null) {
                    streamRegistry.put(id, new HashMap<>());

                    streamRegistry.get(id).put(smrMeta.buf, currentAddress);
                } else {

                    if (streamRegistry.get(id).containsKey(smrMeta.buf)) {
                        // ignore stale entry
                        conflictSMR++;
                        staleBytes += smrMeta.getLen();
                        staleStreamEntries++;
                    } else {
                        // new entry add it to the registry
                        streamRegistry.get(id).put(smrMeta.getBuf(), currentAddress);
                    }
                }
            }

            // If the container is empty free those bytes too
            if (staleStreamEntries == smrs.size()) {
                //staleBytes += Integer.BYTES;
                //staleBytes += Long.BYTES * 2;
                //staleMultiObjectSMR++;
            }
        }

        if (staleMultiObjectSMR == numMultiObjs) {
            //staleBytes += Integer.BYTES;
        }
    }


    List<SMRMeta> parseMultiSMR(ByteBuffer buf) {

        byte magic = buf.get();

        if (magic != 0x42) {
            throw new IllegalStateException("Couldn't find magic");
        }

        byte type = buf.get();

        if (type != 8) {
            throw new IllegalStateException("Expectected a multi smr entry, but type mismatch");
        }

        int smrNum = buf.getInt();
        List<SMRMeta> smrs = new ArrayList<>();

        for (int x = 0; x < smrNum; x++) {
            smrs.add(parseSMR(buf));
        }

        return smrs;
    }

    SMRMeta parseSMR(ByteBuffer buf) {
        totalSMR++;
        int p1 = buf.position();

        byte magic = buf.get();

        if (magic != 0x42) {
            throw new IllegalStateException("Couldn't find magic");
        }

        // get type
        byte type = buf.get();

        if (type != 1) {
            throw new IllegalStateException("Expectected an smr entry, but type mismatch");
        }

        // method len and method string
        short methodLen = buf.getShort();
        buf.position(buf.position() + methodLen);

        // serializer type
        buf.get();

        int numArgs = buf.get();

        // get the conflict key
        int firstParamLen = buf.getInt();
        byte[] conflictKeyBytes = new byte[firstParamLen];

        buf.get(conflictKeyBytes);

        // Skip rest of bytes
        for (int x = 1; x < numArgs; x++) {
            int argLen = buf.getInt();
            buf.position(buf.position() + argLen);
        }

        int p2 = buf.position();

        return new SMRMeta(ByteBuffer.wrap(conflictKeyBytes), (p2 - p1));
    }

    @Data
    class SMRMeta {
        final ByteBuffer buf;
        final int len;
    }
}
