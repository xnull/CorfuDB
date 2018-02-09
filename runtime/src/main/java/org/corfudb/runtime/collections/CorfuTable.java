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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This is a wrapper class that instruments the calls to Accessors, Mutators, and
 * MutatorAccessor and forwards them the CorfuTableCore.
 *
 * Created by Sam Behnam.
 */
@CorfuObject
public class CorfuTable<K ,V, F extends Enum<F> & CorfuTable.IndexSpecification, I>
        extends CorfuTableCore<K, V, F, I> implements ICorfuMap<K, V> {

    private final String mapId;

    static <K, V, F extends Enum<F> & CorfuTable.IndexSpecification, I>
    TypeToken<CorfuTable<K, V, F, I>>
    getTableType() {
        return new TypeToken<CorfuTable<K, V, F, I>>() {};
    }

    static <K, V> TypeToken<CorfuTable<K, V, NoSecondaryIndex, Void>>
    getMapType() {
        return new TypeToken<CorfuTable<K, V, NoSecondaryIndex, Void>>() {};
    }

    /**
     * The interface for an indexing function.
     * @param <K> The type of the key used in indexing.
     * @param <V> The type of the value used in indexing.
     * @param <I> The type of index key.
     */
    @FunctionalInterface
    public interface IndexFunction<K, V, I> {
        @Nonnull Collection<I> generateIndex(K k, V v);
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
    public interface IndexSpecification<K, V, I, P> {
        CorfuTable.IndexFunction<K, V, I> getIndexFunction();
        CorfuTable.ProjectionFunction<K, V, I, P> getProjectionFunction();
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

    public CorfuTable(Class<F> indexFunctionEnumClass) {
        super(indexFunctionEnumClass);
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
        // Calls to put are recorded at the CorfuCompileProxy
        return super.put(key, value);
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "clear", reset = true)
    public void clear() {
        super.clear();
        Auditor.INSTANCE.addEvent(Event.Type.OPCLEAR.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public int size() {
        final int result = super.size();
        Auditor.INSTANCE.addEvent(Event.Type.OPSIZE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public V get(@ConflictParameter Object key) {
        final V result = super.get(key);
        Auditor.INSTANCE.addEvent(Event.Type.OPGET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), key);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean isEmpty() {
        final boolean result = super.isEmpty();
        Auditor.INSTANCE.addEvent(Event.Type.OPISEMPTY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsKey(@ConflictParameter Object key) {
        final boolean result = super.containsKey(key);
        Auditor.INSTANCE.addEvent(Event.Type.OPCONTAINSKEY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), key);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsValue(Object value) {
        final boolean result = super.containsValue(value);
        Auditor.INSTANCE.addEvent(Event.Type.OPCONTAINSVALUE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), value);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public List<V> scanAndFilter(Predicate<? super V> p) {
        final List<V> result = super.scanAndFilter(p);
        Auditor.INSTANCE.addEvent(Event.Type.OPSCANFILTER.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public Collection<Entry<K, V>> scanAndFilterByEntry(Predicate<? super Entry<K, V>> entryPredicate) {
        final Collection<Entry<K, V>> result = super.scanAndFilterByEntry(entryPredicate);
        Auditor.INSTANCE.addEvent(Event.Type.OPSCANFILTERENTRY.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
    public V remove(@ConflictParameter Object key) {
        final V result = super.remove(key);
        Auditor.INSTANCE.addEvent(Event.Type.OPREMOVE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), key);
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "putAll",
            undoFunction = "undoPutAll",
            undoRecordFunction = "undoPutAllRecord",
            conflictParameterFunction = "putAllConflictFunction")
    public void putAll(@Nonnull Map<? extends K, ? extends V> m) {
        super.putAll(m);
        Auditor.INSTANCE.addEvent(Event.Type.OPPUTALL.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), m.values());
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Set<K> keySet() {
        final Set<K> result = super.keySet();
        Auditor.INSTANCE.addEvent(Event.Type.OPKEYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Collection<V> values() {
        final Collection<V> result = super.values();
        Auditor.INSTANCE.addEvent(Event.Type.OPVLAUES.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public Set<Entry<K, V>> entrySet() {
        final Set<Entry<K, V>> result = super.entrySet();
        Auditor.INSTANCE.addEvent(Event.Type.OPENTRYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
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
    @Nonnull
    @Override
    @Accessor
    public <P> Collection<P> getByIndex(@Nonnull F indexFunction, I index) {
        return super.getByIndex(indexFunction, index);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public <P> Collection<P> getByIndexAndFilter(@Nonnull F indexFunction,
                                                 @Nonnull ProjectionFunction<K, V, I, P> projectionFunction,
                                                 @Nonnull Predicate<? super Entry<K, V>> entryPredicate, I index) {
        return super.getByIndexAndFilter(indexFunction, projectionFunction, entryPredicate, index);
    }

    /** {@inheritDoc} */
    @Nonnull
    @Override
    @Accessor
    public <P> Collection<P> getByIndex(@Nonnull F indexFunction,
                                        @Nonnull ProjectionFunction<K, V, I, P> projectionFunction, I index) {
        return super.getByIndex(indexFunction, projectionFunction, index);
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
