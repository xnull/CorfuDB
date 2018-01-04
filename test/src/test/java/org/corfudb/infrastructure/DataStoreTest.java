package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableMap;

import java.util.UUID;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.test.CorfuTest;
import org.corfudb.test.parameters.Param;
import org.corfudb.test.parameters.Parameter;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.test.parameters.Param.TEST_TEMP_DIR;

/**
 * Created by mdhawan on 7/29/16.
 */
@CorfuTest
public class DataStoreTest {

    @CorfuTest
    public void testPutGet(@Parameter(TEST_TEMP_DIR) String serviceDir) {
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
    }

    @CorfuTest
    public void testPutGetWithRestart(@Parameter(TEST_TEMP_DIR) String serviceDir) {
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);

        //Simulate a restart of data store
        dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
    }

    @CorfuTest
    public void testDatastoreEviction(@Parameter(TEST_TEMP_DIR) String serviceDir) {
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--log-path", serviceDir)
                .build());

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(String.class, "test", "key", value);

            //Simulate a restart of data store
            dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                    .put("--log-path", serviceDir)
                    .build());
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);
            dataStore.put(String.class, "test", "key", "NEW_VALUE");
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
        }
    }

    @CorfuTest
    public void testInmemoryPutGet(@Parameter(TEST_TEMP_DIR) String serviceDir) {
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--memory", true)
                .build());
        String value = UUID.randomUUID().toString();
        dataStore.put(String.class, "test", "key", value);
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

        dataStore.put(String.class, "test", "key", "NEW_VALUE");
        assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");

    }

    @CorfuTest
    public void testInmemoryEviction(@Parameter(TEST_TEMP_DIR) String serviceDir) {
        DataStore dataStore = new DataStore(new ImmutableMap.Builder<String, Object>()
                .put("--memory", true)
                .build());

        for (int i = 0; i < dataStore.getDsCacheSize() * 2; i++) {
            String value = UUID.randomUUID().toString();
            dataStore.put(String.class, "test", "key", value);
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo(value);

            dataStore.put(String.class, "test", "key", "NEW_VALUE");
            assertThat(dataStore.get(String.class, "test", "key")).isEqualTo("NEW_VALUE");
        }
    }
}
