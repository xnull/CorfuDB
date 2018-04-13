package org.corfudb.util.auditor;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Sam Behnam on 2/21/18.
 */
@Slf4j
public final class AuditorConfiguration {
    private static final Properties properties;

    public static final boolean TO_PERSIST_SERIALIZED;
    public static final boolean TO_PERSIST_TEXT;
    public static final int DUMP_TIME_PERIOD;
    public static final long NOT_AVAILABLE_DURATION;
    public static final String NOT_AVAILABLE;
    public static final String EXTENSION_LIST;
    public static final String EXTENSION_TEXT;
    public static final String PATH_TO_RECORDING_FILE;
    public static final String SHUTDOWN_DUMP;

    static {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream input = classLoader.getResourceAsStream("auditor.properties");
        properties = new Properties();
        try {
            properties.load(input);
        } catch (IOException e) {
            log.error("Error processing properties of {}: {}",input.toString(), e.toString());
        }

        TO_PERSIST_SERIALIZED = Boolean.parseBoolean(properties.getProperty("toPersistSerialized", "True"));
        TO_PERSIST_TEXT = Boolean.parseBoolean(properties.getProperty("toPersistText", "True"));
        DUMP_TIME_PERIOD = Integer.parseInt(properties.getProperty("dumpTimePeriod", "180"));
        NOT_AVAILABLE_DURATION = Long.parseLong(properties.getProperty("notAvailableDuration", "-1"));
        NOT_AVAILABLE = properties.getProperty("notAvailable", "N/A");
        EXTENSION_LIST = properties.getProperty("extensionList", ".list");
        EXTENSION_TEXT = properties.getProperty("extensionText", ".txt");
        PATH_TO_RECORDING_FILE = properties.getProperty("pathToRecordingFile", "/tmp/recording");
        SHUTDOWN_DUMP = properties.getProperty("shutdownDump", "/tmp/shutdown-dump");
    }
}
