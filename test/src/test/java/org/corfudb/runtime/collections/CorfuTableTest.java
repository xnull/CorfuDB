package org.corfudb.runtime.collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.assertj.core.data.MapEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTableTest.StringIndexers;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.test.CorfuTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by mwei on 1/7/16.
 */
@CorfuTest
public class CorfuTableTest implements IMapTest<CorfuTable> {

    @RequiredArgsConstructor
    public enum StringIndexers implements CorfuTable.IndexSpecification<String, String, String, String> {
        BY_VALUE((k,v) -> Collections.singleton(v)),
        BY_FIRST_LETTER((k, v) -> Collections.singleton(Character.toString(v.charAt(0))))
        ;

        @Getter
        final CorfuTable.IndexFunction<String, String, String> indexFunction;

        @Getter
        final CorfuTable.ProjectionFunction<String, String, String, String> projectionFunction
            = (i, s) -> s.map(entry -> entry.getValue());
    }

    @RequiredArgsConstructor
    enum OtherStringIndexer implements CorfuTable.IndexSpecification<String, String, String, String> {
        BY_LAST_LETTER((k, v) -> Collections.singleton(Character.toString(v.charAt(v.length()-1))));
        ;

        @Getter
        final CorfuTable.IndexFunction<String, String, String> indexFunction;

        @Getter
        final CorfuTable.ProjectionFunction<String, String, String, String> projectionFunction
            = (i, s) -> s.map(entry -> entry.getValue());
    }


    @CorfuTest
    public void openingCorfuTableTwice(CorfuRuntime runtime) {
        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
            instance1 = runtime.getObjectsView().build()
            .setTypeToken(
                new TypeToken<CorfuTable<String, String, CorfuTableTest.StringIndexers, String>>() {})
            .setArguments(CorfuTableTest.StringIndexers.class)
            .setStreamName("test")
            .open();

        assertThat(instance1.hasSecondaryIndices()).isTrue();

        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
            instance2 = runtime.getObjectsView().build()
            .setTypeToken(
                new TypeToken<CorfuTable<String, String, CorfuTableTest.StringIndexers, String>>() {})
            .setStreamName("test")
            .open();

        // Verify that the first the indexer is set on the first open
        // TODO(Maithem): This might seem like weird semantics, but we
        // address it once we tackle the lifecycle of SMRObjects.
        assertThat(instance2.getIndexerClass()).isEqualTo(instance1.getIndexerClass());
    }

    @CorfuTest
    @SuppressWarnings("unchecked")
    public void canReadFromEachIndex(CorfuRuntime runtime) {
        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
            corfuTable = runtime.getObjectsView().build()
            .setTypeToken(
                new TypeToken<CorfuTable<String, String, CorfuTableTest.StringIndexers, String>>() {})
            .setArguments(CorfuTableTest.StringIndexers.class)
            .setStreamName("test")
            .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        assertThat(corfuTable.getByIndex(CorfuTableTest.StringIndexers.BY_FIRST_LETTER, "a"))
            .containsExactly("a", "ab");

        assertThat(corfuTable.getByIndex(CorfuTableTest.StringIndexers.BY_VALUE, "ab"))
            .containsExactly("ab");
    }


    @CorfuTest
    @SuppressWarnings("unchecked")
    public void emptyIndexesReturnEmptyValues(CorfuRuntime runtime) {
        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
            corfuTable = runtime.getObjectsView().build()
            .setTypeToken(
                new TypeToken<CorfuTable<String, String, CorfuTableTest.StringIndexers, String>>() {})
            .setArguments(CorfuTableTest.StringIndexers.class)
            .setStreamName("test")
            .open();


        assertThat(corfuTable.getByIndex(CorfuTableTest.StringIndexers.BY_FIRST_LETTER, "a"))
            .isEmpty();

        assertThat(corfuTable.getByIndex(CorfuTableTest.StringIndexers.BY_VALUE, "ab"))
            .isEmpty();
    }


    @CorfuTest
    @SuppressWarnings("unchecked")
    public void canReadWithoutIndexes(CorfuRuntime runtime) {
        CorfuTable<String, String, CorfuTable.NoSecondaryIndex, Void>
            corfuTable = runtime.getObjectsView().build()
            .setTypeToken(
                new TypeToken<CorfuTable<String, String,
                    CorfuTable.NoSecondaryIndex, Void>>() {})
            .setArguments(CorfuTableTest.StringIndexers.class)
            .setStreamName("test")
            .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");

        assertThat(corfuTable)
            .containsExactly(MapEntry.entry("k1", "a"),
                MapEntry.entry("k2", "ab"),
                MapEntry.entry("k3", "b"));
    }

    /**
     * Remove an entry also update indices
     */
    @CorfuTest
    public void doUpdateIndicesOnRemove(CorfuRuntime runtime) throws Exception {
        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
            corfuTable = runtime.getObjectsView().build()
            .setTypeToken(CorfuTable.<String, String, CorfuTableTest.StringIndexers, String>getTableType())
            .setArguments(CorfuTableTest.StringIndexers.class)
            .setStreamName("test")
            .open();

        corfuTable.put("k1", "a");
        corfuTable.put("k2", "ab");
        corfuTable.put("k3", "b");
        corfuTable.remove("k2");

        assertThat(corfuTable.getByIndex(CorfuTableTest.StringIndexers.BY_FIRST_LETTER, "a"))
            .containsExactly("a");
    }

}
