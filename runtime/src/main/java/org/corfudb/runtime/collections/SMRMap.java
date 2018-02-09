package org.corfudb.runtime.collections;

import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.util.auditor.Auditor;
import org.corfudb.util.auditor.Event;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * This is a wrapper class that instruments the calls to Accessors, Mutators, and
 * MutatorAccessor and forwards them the SMRMapCore.
 *
 * Created by Sam Behnam.
 */
@CorfuObject
@Deprecated
@SuppressWarnings("checkstyle:abbreviation")
public class SMRMap<K, V> extends SMRMapCore<K, V> implements ISMRMap<K, V> {

    private final String mapId;

    public SMRMap() {
        super();
        mapId = String.valueOf(System.identityHashCode(this));
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "clear", reset = true)
    public void clear() {
        super.clear();
        Auditor.INSTANCE.addEvent(Event.Type.OPCLEAR.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId())
        );
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "put", noUpcall = true)
    public void blindPut(K key, V value) {
        super.blindPut(key, value);
        Auditor.INSTANCE.addEvent(Event.Type.OPBLINDPUT.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), key, value);
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public boolean containsKey(Object key) {
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
    public Set<Entry<K, V>> entrySet() {
        final Set<Entry<K, V>> result = super.entrySet();
        Auditor.INSTANCE.addEvent(Event.Type.OPENTRYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @Accessor
    public V get(Object key) {
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
    public Set<K> keySet() {
        final Set<K> result = super.keySet();
        Auditor.INSTANCE.addEvent(Event.Type.OPKEYSET.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    public V put(K key, V value) {
        // In order to avoid recording put events as part of undoPut,
        // recording a put event will take place in CorfuCompileProxy
        return super.put(key, value);
    }

    @Override
    public V removeUninstrumented(K key) {
        return super.remove(key);
    }

    @Override
    public V getUninstrumented(K key) {
        return super.get(key);
    }

    /** {@inheritDoc} */
    @Override
    @Mutator(name = "putAll", undoFunction = "undoPutAll",
            undoRecordFunction = "undoPutAllRecord",
            conflictParameterFunction = "putAllConflictFunction")
    public void putAll(Map<? extends K, ? extends V> m) {
        super.putAll(m);
        Auditor.INSTANCE.addEvent(Event.Type.OPPUTALL.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
    }

    /** {@inheritDoc} */
    @Override
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
            undoRecordFunction = "undoRemoveRecord")
    public V remove(Object key) {
        final V result = super.remove(key);
        Auditor.INSTANCE.addEvent(Event.Type.OPREMOVE.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()), key);
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
    public Collection<V> values() {
        final Collection<V> result = super.values();
        Auditor.INSTANCE.addEvent(Event.Type.OPVLAUES.getTypeValue(),
                mapId,
                String.valueOf(Thread.currentThread().getId()));
        return result;
    }
}
