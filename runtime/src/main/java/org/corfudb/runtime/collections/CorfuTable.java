package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.util.auditor.Auditor;
import org.corfudb.util.auditor.Event;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * This is a wrapper class that instruments the calls to Accessors, Mutators, and
 * MutatorAccessor and forwards them the CorfuTableCore.
 *
 * Created by Sam Behnam.
 */
@CorfuObject
public class CorfuTable<K ,V> extends CorfuTableCore<K, V> implements ICorfuMap<K, V> {

    private final String mapId;

    /**
     * Denotes a function that supplies the unique name of an index registered to
     * {@link CorfuTable}.
     */
    @FunctionalInterface
    public interface IndexName extends Supplier<String> {
    }

    /**
     * Denotes a function that takes as input the key and value of an {@link CorfuTable}
     * record, and computes the associated index value for the record.
     *
     * @param <K> type of the record key.
     * @param <V> type of the record value.
     * @param <I> type of the index value computed.
     */
    @FunctionalInterface
    public interface IndexFunction<K, V, I extends Comparable<?>> extends BiFunction<K, V, I> {
    }

    /**
     * Descriptor of named {@link CorfuTable.IndexFunction} entry.
     *
     * @param <K> type of the record key associated with {@code IndexKey}.
     * @param <V> type of the record value associated with {@code IndexKey}.
     * @param <I> type of the index value computed using the {@code IndexKey}.
     */
    public static class Index<K, V, I extends Comparable<?>> {
        private CorfuTable.IndexName name;
        private CorfuTable.IndexFunction<K, V, I> indexFunction;

        public Index(CorfuTable.IndexName name, CorfuTable.IndexFunction<K, V, I> indexFunction) {
            this.name = name;
            this.indexFunction = indexFunction;
        }

        public CorfuTable.IndexName getName() {
            return name;
        }

        public CorfuTable.IndexFunction<K, V, I> getIndexFunction() {
            return indexFunction;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof CorfuTable.Index)) return false;
            CorfuTable.Index<?, ?, ?> index = (CorfuTable.Index<?, ?, ?>) o;
            return Objects.equals(name.get(), index.name.get());
        }

        @Override
        public int hashCode() {
            return Objects.hash(name.get());
        }
    }

    /**
     * Registry hosting of a collection of {@link CorfuTable.Index}.
     *
     * @param <K> type of the record key associated with {@code Index}.
     * @param <V> type of the record value associated with {@code Index}.
     */
    public interface IndexRegistry<K, V>
            extends Iterable<CorfuTable.Index<K, V, ? extends Comparable<?>>> {

        CorfuTable.IndexRegistry<?, ?> EMPTY = new CorfuTable.IndexRegistry<Object, Object>() {
            @Override
            public <I extends Comparable<?>> Optional<CorfuTable.IndexFunction<Object, Object, I>> get(CorfuTable.IndexName name) {
                return Optional.empty();
            }

            @Override
            public Iterator<CorfuTable.Index<Object, Object, ? extends Comparable<?>>> iterator() {
                return Collections.emptyIterator();
            }
        };

        /**
         * Obtain the {@link CorfuTable.IndexFunction} via its registered {@link CorfuTable.IndexName}.
         *
         * @param name name of the {@code IndexKey} previously registered.
         * @param <I>  type of the {@code IndexKey}.
         * @return the instance of {@link CorfuTable.IndexFunction} registered to the lookup name.
         */
        <I extends Comparable<?>> Optional<CorfuTable.IndexFunction<K, V, I>> get(CorfuTable.IndexName name);

        /**
         * Obtain a static {@link CorfuTable.IndexRegistry} with no registered {@link CorfuTable.IndexFunction}s.
         *
         * @param <K> type of the record key associated with {@code Index}.
         * @param <V> type of the record value associated with {@code Index}.
         * @return a static instance of {@link CorfuTable.IndexRegistry}.
         */
        static <K, V> CorfuTable.IndexRegistry<K, V> empty() {
            @SuppressWarnings("unchecked")
            CorfuTable.IndexRegistry<K, V> result = (CorfuTable.IndexRegistry<K, V>) EMPTY;
            return result;
        }
    }

    /** Helper function to get a map (non-secondary index) Corfu table.
     *
     * @param <K>           Key type
     * @param <V>           Value type
     * @return              A type token to pass to the builder.
     */
    static <K, V> TypeToken<CorfuTable<K, V>>
    getMapType() {
        return new TypeToken<CorfuTable<K, V>>() {};
    }

    /** Helper function to get a Corfu Table.
     *
     * @param <K>                   Key type
     * @param <V>                   Value type
     * @return                      A type token to pass to the builder.
     */
    static <K, V>
    TypeToken<CorfuTable<K, V>>
    getTableType() {
        return new TypeToken<CorfuTable<K, V>>() {};
    }

    /**
     * The interface for a projection function.
     *
     * <p> NOTE: The projection function MUST return a new (preferably immutable) collection,
     * the collection of entries passed during this function are NOT safe to use
     * outside the context of this function.
     *
     * @param <K>   The type of the key used in projection.
     * @param <V>   The type of the value used in projection.
     * @param <I>   The type of the index used in projection.
     * @param <P>   The type of the projection returned.
     */
    @FunctionalInterface
    public interface ProjectionFunction<K, V, I, P> {
        @Nonnull
        Stream<P> generateProjection(I index,
                                     @Nonnull Stream<Map.Entry<K, V>> entryStream);
    }

    /**
     * The interface for a index specification, which consists of a indexing function
     * and a projection function.
     * @param <K>   The type of the primary key on the map.
     * @param <V>   The type of the value on the map.
     * @param <I>   The type of the index.
     * @param <P>   The type returned by the projection function.
     */
    public interface IndexSpecification<K, V, I extends Comparable<?>, P> {
        IndexFunction<K, V, I> getIndexFunction();
        ProjectionFunction<K, V, I, P> getProjectionFunction();
    }

    /** An index specification enumeration which has no index specifications.
     *  Using this enumeration effectively disables secondary indexes.
     */
    enum NoSecondaryIndex implements IndexSpecification {
        ;

        @Override
        public IndexFunction getIndexFunction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public ProjectionFunction getProjectionFunction() {
            throw new UnsupportedOperationException();
        }
    }

    public CorfuTable(IndexRegistry<K, V> indices) {
        super(indices);
        mapId = String.valueOf(System.identityHashCode(this));
    }

    public CorfuTable() {
        super();
        mapId = String.valueOf(System.identityHashCode(this));
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    public V put(@ConflictParameter K key, V value) {
        // In order to avoid recording put events as part of undoPut,
        // recording a put event will take place at logUpdate in CorfuCompileProxy
        return super.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "clear", reset = true)
    public void clear() {
        // In order to avoid recording redundant clear events, recording
        // a clear event will take place at logUpdate in CorfuCompileProxy
        super.clear();
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public int size() {
        final long startTime = System.nanoTime();
        final int result = super.size();
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPSIZE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public V get(@ConflictParameter Object key) {

        final long startTime = System.nanoTime();
        final V result = super.get(key);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPGET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration, key);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean isEmpty() {

        final long startTime = System.nanoTime();
        final boolean result = super.isEmpty();
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPISEMPTY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsKey(@ConflictParameter Object key) {

        final long startTime = System.nanoTime();
        final boolean result = super.containsKey(key);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPCONTAINSKEY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration,
                key);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsValue(Object value) {

        final long startTime = System.nanoTime();
        final boolean result = super.containsValue(value);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPCONTAINSVALUE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration,
                value);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public List<V> scanAndFilter(Predicate<? super V> p) {

        final long startTime = System.nanoTime();
        final List<V> result = super.scanAndFilter(p);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPSCANFILTER.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {

        final long startTime = System.nanoTime();
        final Collection<Entry<K, V>> result = super.scanAndFilterByEntry(entryPredicate);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPSCANFILTERENTRY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
    public V remove(@ConflictParameter Object key) {
        // In order to avoid recording remove events as part of undoRemove,
        // recording a remove event will take place at logUpdate in CorfuCompileProxy
        return super.remove(key);
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "putAll",
            undoFunction = "undoPutAll",
            undoRecordFunction = "undoPutAllRecord",
            conflictParameterFunction = "putAllConflictFunction")
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {

        final long startTime = System.nanoTime();
        super.putAll(m);
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPPUTALL.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration,
                m.values());
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Set<K> keySet() {

        final long startTime = System.nanoTime();
        final Set<K> result = super.keySet();
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPKEYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Collection<V> values() {

        final long startTime = System.nanoTime();
        final Collection<V> result = super.values();
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPVLAUES.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Set<Entry<K, V>> entrySet() {

        final long startTime = System.nanoTime();
        final Set<Entry<K, V>> result = super.entrySet();
        final long duration = System.nanoTime() - startTime;

        Auditor.INSTANCE.addEvent(Event.Type.OPENTRYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()),
                duration);
        return result;
    }

    // The following methods are not currently instrumented and delegate to CorfuTableCore
    // for handling the calls

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "put", noUpcall = true)
    public void insert(@ConflictParameter K key, V value) {
        // Not insturmented as it leads to an instrumented put
        super.insert(key, value);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Entry<K, V>> getByIndex(@Nonnull IndexName indexName, I indexKey) {
        return super.getByIndex(indexName, indexKey);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public @Nonnull
    <I extends Comparable<I>>
    Collection<Map.Entry<K, V>> getByIndexAndFilter(@Nonnull CorfuTable.IndexName indexName,
                                                    @Nonnull Predicate<? super Entry<K, V>>
                                                            entryPredicate,
                                                    I indexKey) {
        return super.getByIndexAndFilter(indexName, entryPredicate, indexKey);
    }



    /** {@inheritDoc} */
    @Override
    @Mutator(name = "remove", noUpcall = true)
    public void delete(@ConflictParameter K key) {
        super.delete(key);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public V getOrDefault(Object key, V defaultValue) {
        return super.getOrDefault(key, defaultValue);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public void forEach(BiConsumer<? super K, ? super V> action) {
        super.forEach(action);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean hasSecondaryIndices() {
        return super.hasSecondaryIndices();
    }
}
