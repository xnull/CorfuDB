package org.corfudb.recovery.stream;

import com.google.common.annotations.VisibleForTesting;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTable.IndexRegistry;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.view.ObjectBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Builder
public class CustomTypeStreams {

    @Default
    @NonNull
    @Getter
    private final Class<?> defaultObjectsType = SMRMap.class;

    /**
     * We can register streams with non-default type
     */
    @Default
    @NonNull
    private final Map<UUID, ObjectBuilder<?>> customTypeStreams = new HashMap<>();

    @VisibleForTesting
    public void addCustomTypeStream(UUID streamId, ObjectBuilder<?> ob) {
        customTypeStreams.put(streamId, ob);
    }

    public Optional<ObjectBuilder<?>> get(UUID streamId) {
        return Optional.ofNullable(customTypeStreams.get(streamId));
    }

    /**
     * Add an indexer to a stream (that backs a CorfuTable)
     *
     * @param streamName    Stream name.
     * @param indexRegistry Index Registry.
     */
    public void addIndexerToCorfuTableStream(String streamName, IndexRegistry<?, ?> indexRegistry, CorfuRuntime runtime) {
        UUID streamId = CorfuRuntime.getStreamID(streamName);
        ObjectBuilder<?> ob = new ObjectBuilder(runtime)
                .setArguments(indexRegistry)
                .setStreamID(streamId)
                .setType(CorfuTable.class);

        addCustomTypeStream(streamId, ob);
    }

    public Class getStreamType(UUID streamId) {
        if (customTypeStreams.containsKey(streamId)) {
            return customTypeStreams.get(streamId).getType();
        }

        return defaultObjectsType;
    }
}
