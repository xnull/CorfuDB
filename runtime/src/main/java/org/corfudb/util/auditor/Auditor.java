package org.corfudb.util.auditor;

import lombok.Getter;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A singleton object used to collect events on CorfuTable and SMRMap method
 * calls The results will be drained and dumped periodically and at shutdown
 * as configured in this class
 *
 * Created by Sam Behnam
 */

@SuppressWarnings("checkstyle:printStackTrace") // Instrumentation code
public enum Auditor {
    INSTANCE;

    public static final boolean TO_PERSIST_SERIALIZED = true;
    public static final boolean TO_PERSIST_TEXT = true;
    public static final int DUMP_TIME_PERIOD = 3;
    public static final String EXTENSION_LIST = ".list";
    public static final String EXTENSION_TEXT = ".txt";
    public static final String NOT_AVAILABLE = "N/A";
    public static final String PATH_TO_RECORDING_FILE = "/tmp/recording";
    public static final String SHUTDOWN_DUMP = "/tmp/shutdown-dump";

    @Getter
    Queue<Event> eventQueue = new ConcurrentLinkedQueue<>();

    Auditor() {
        // Registering a shutdown hook to persist audit data at JVM shutdown
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                Auditor.INSTANCE.dump(SHUTDOWN_DUMP + System.nanoTime());
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
        service.scheduleAtFixedRate(dumpRunnable, DUMP_TIME_PERIOD, DUMP_TIME_PERIOD, TimeUnit.MINUTES);
    }

    // Add an event representing a call to a CorfuTable or SMRMap
    public void addEvent(String type, String mapId, String threadId, Object... args) {
        String txId = NOT_AVAILABLE;
        String txType = NOT_AVAILABLE;
        if (TransactionalContext.isInTransaction()) {
            txId = TransactionalContext.getCurrentContext().getTransactionID().toString();
            txType = TransactionalContext.getCurrentContext().getBuilder().getType().name();
        }
        eventQueue.add(Event.newEventInstance(type, mapId, threadId, txId, txType, args));
    }

    // A convenience method to add event when mapId does not exist for a call to a CorfuTable or SMRMap
    public void addEvent(String typeValue, String threadId) {
        addEvent(typeValue, NOT_AVAILABLE, threadId);
    }

    private void dump() {
        dump(PATH_TO_RECORDING_FILE + System.nanoTime());
    }

    private void dump(String pathToFile) {
        List<Event> collectedEventsInInterval = new ArrayList<>();
        int size = eventQueue.size();
        for (int i = 0; i < size; i++) {
            Event event = eventQueue.poll();

            // size of concurrent queue might decrease and lead to offer method returning null
            if (event == null) break;

            collectedEventsInInterval.add(event);
        }

        // Will persist according to setting values of constants
        persistSerialized(pathToFile, collectedEventsInInterval);
        persistInPlainText(pathToFile, collectedEventsInInterval);
    }

    private void persistInPlainText(String pathToFile, List<Event> collectedEventsInInterval) {
        if (TO_PERSIST_TEXT) {
            String pathToTextFile = pathToFile + EXTENSION_TEXT;
            try {
                PrintStream printStream = new PrintStream(new File(pathToTextFile));
                for (Event event : collectedEventsInInterval) {
                    printStream.println(event.toString());
                }
                printStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void persistSerialized(String pathToFile, List<Event> collectedEventsInInterval) {
        if (TO_PERSIST_SERIALIZED) {
            String pathToSerializedFile = pathToFile + EXTENSION_LIST;
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(pathToSerializedFile);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                objectOutputStream.writeObject(collectedEventsInInterval);
                objectOutputStream.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
