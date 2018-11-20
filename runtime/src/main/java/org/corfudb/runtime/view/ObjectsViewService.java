package org.corfudb.runtime.view;

import org.corfudb.runtime.view.ObjectsView.ObjectID;

import java.util.function.Function;

interface ObjectsViewService {
    void clear();

    <T> boolean contains(ObjectID<T> objectId);

    <T> T computeIfAbsent(ObjectID<T> objectId, Function<ObjectID<?>, ?> mappingFunction);

    <T> T get(ObjectID<T> id);

    int size();
}
