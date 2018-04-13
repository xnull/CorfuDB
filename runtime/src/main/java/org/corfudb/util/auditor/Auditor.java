package org.corfudb.util.auditor;

import lombok.Getter;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A singleton object used to collect events on CorfuTable and SMRMap method
 * calls The results will be drained and dumped periodically and at shutdown
 * as configured in AudioConfiguration class
 *
 * Created by Sam Behnam
 */

@SuppressWarnings("checkstyle:printStackTrace") // Instrumentation code
public enum Auditor {
    INSTANCE;

    @Getter
    Queue<Event> eventQueue = new ConcurrentLinkedQueue<>();
    private volatile int dumpedEventSize;

    Auditor() {
        // Registering a shutdown hook to persist audit data at JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Auditor.INSTANCE.dump(AuditorConfiguration.SHUTDOWN_DUMP
                        + System.nanoTime());
            }
        });

        // Scheduling periodic draining and dumping of recorded events
        Runnable dumpRunnable = new Runnable() {
            @Override
            public void run() {
                Auditor.INSTANCE.dump();
            }
        };

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(dumpRunnable, AuditorConfiguration.DUMP_TIME_PERIOD,
                AuditorConfiguration.DUMP_TIME_PERIOD, TimeUnit.SECONDS);
    }

    // Add an event representing a call to a CorfuTable or SMRMap
    public void addEvent(String type, String mapId, String threadId, long duration, Object... args) {
        String txId = AuditorConfiguration.NOT_AVAILABLE;
        String txType = AuditorConfiguration.NOT_AVAILABLE;
        String txSnapshotTimestamp = AuditorConfiguration.NOT_AVAILABLE;

        // Collect transaction information if available
        if (TransactionalContext.isInTransaction()) {
            txId = TransactionalContext.getCurrentContext().getTransactionID().toString();
            txType = TransactionalContext.getCurrentContext().getBuilder().getType().name();
            txSnapshotTimestamp = String.valueOf(TransactionalContext.getCurrentContext().getSnapshotTimestamp());
        }

        String durationString = duration >=  0 ?
                String.valueOf(duration):
                AuditorConfiguration.NOT_AVAILABLE;

        eventQueue.add(Event.newEventInstance(type, mapId, threadId,
                durationString, txId, txType, txSnapshotTimestamp, args));
    }

    // A convenience method to add event when mapId and duration do not exist.
    // Can be used for transaction markers (i.e. TXBegin, TXEnd, TXAbort)
    public void addEvent(String typeValue, String threadId) {
        addEvent(typeValue, AuditorConfiguration.NOT_AVAILABLE,
                threadId, AuditorConfiguration.NOT_AVAILABLE_DURATION);
    }

    public int numberOfRecordedEvents() {
        return eventQueue.size() + dumpedEventSize;
    }

    // Convenient method for dumping the current content of added event
    public void dump() {
        dump(AuditorConfiguration.PATH_TO_RECORDING_FILE + System.nanoTime());
    }

    public void dump(String pathToFile) {
        List<Event> collectedEventsInInterval = new ArrayList<>();
        int size = eventQueue.size();
        for (int i = 0; i < size; i++) {
            Event event = eventQueue.poll();

            // size of concurrent queue might decrease and lead to offer method returning null
            if (event == null) break;

            collectedEventsInInterval.add(event);
        }

        dumpedEventSize += size;

        // Will persist according to setting values of constants
        persistSerialized(pathToFile, collectedEventsInInterval);
        persistInPlainText(pathToFile, collectedEventsInInterval);
    }

    private void persistInPlainText(String pathToFile, List<Event> collectedEventsInInterval) {
        if (AuditorConfiguration.TO_PERSIST_TEXT) {
            String pathToTextFile = pathToFile + AuditorConfiguration.EXTENSION_TEXT;
            try (PrintStream printStream = new PrintStream(new File(pathToTextFile))){
                for (Event event : collectedEventsInInterval) {
                    printStream.println(event.toString());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void persistSerialized(String pathToFile, List<Event> collectedEventsInInterval) {
        if (AuditorConfiguration.TO_PERSIST_SERIALIZED) {
            String pathToSerializedFile = pathToFile + AuditorConfiguration.EXTENSION_LIST;
            try (FileOutputStream fileOutputStream = new FileOutputStream(pathToSerializedFile);
                 ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)){
                objectOutputStream.writeObject(collectedEventsInInterval);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
