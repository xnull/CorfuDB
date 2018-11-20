package org.corfudb.recovery;

import lombok.Getter;
import org.corfudb.recovery.FastLoader.FastObjectLoader;
import org.corfudb.recovery.FastLoader.FastObjectLoader.FastObjectLoaderBuilder;
import org.corfudb.recovery.FastLoader.FastSequencerLoader;
import org.corfudb.recovery.FastLoader.FastSequencerLoader.FastSequencerLoaderBuilder;
import org.corfudb.recovery.FastObjectLoaderConfig.FastObjectLoaderConfigBuilder;
import org.corfudb.recovery.address.AddressSpaceLoader;
import org.corfudb.recovery.address.AddressSpaceLoader.AddressSpaceLoaderBuilder;
import org.corfudb.recovery.stream.CustomTypeStreams;
import org.corfudb.recovery.stream.CustomTypeStreams.CustomTypeStreamsBuilder;
import org.corfudb.recovery.stream.StreamLogLoader;
import org.corfudb.recovery.stream.StreamLogLoader.StreamLogLoaderBuilder;
import org.corfudb.recovery.stream.StreamLogLoader.StreamLogLoaderMode;
import org.corfudb.recovery.stream.StreamsMetaDataManager;
import org.corfudb.recovery.stream.StreamsMetaDataManager.StreamsMetaDataManagerBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters.CorfuRuntimeParametersBuilder;

@Getter
public class RecoveryFixture {
    final CorfuRuntimeParametersBuilder runtimeParamsBuilder = CorfuRuntimeParameters.builder();
    final AddressSpaceLoaderBuilder addressLoaderBuilder = AddressSpaceLoader.builder();
    final FastObjectLoaderBuilder fastObjectLoaderBuilder = FastObjectLoader.builder();
    final FastObjectLoaderConfigBuilder configBuilder = FastObjectLoaderConfig.builder();
    final CustomTypeStreamsBuilder customStreamTypeBuilder = CustomTypeStreams.builder();
    final StreamLogLoaderBuilder streamLogLoaderBuilder = StreamLogLoader.builder();
    final StreamsMetaDataManagerBuilder streamsMetadataBuilder = StreamsMetaDataManager.builder();
    final FastSequencerLoaderBuilder fastSequencerLoaderBuilder = FastSequencerLoader.builder();

    public static class CachedRecoveryFixture extends RecoveryFixture {
        @Getter(lazy = true)
        private final FastObjectLoaderConfig config = configBuilder.build();
        @Getter(lazy = true)
        private final CorfuRuntime runtime = runtimeFactory();
        @Getter(lazy = true)
        private final FastObjectLoader fastObjectLoader = folFactory();
        @Getter(lazy = true)
        private final AddressSpaceLoader addressSpaceLoader = addressSpaceLoaderFactory();
        @Getter(lazy = true)
        private final CustomTypeStreams customTypeStreams = customStreamTypeBuilder.build();
        @Getter(lazy = true)
        private final StreamLogLoader streamLoader = streamLogLoaderBuilder.mode(StreamLogLoaderMode.WHITELIST).build();
        @Getter(lazy = true)
        private final StreamsMetaDataManager streamsMetaData = streamsMetadataBuilder.build();
        @Getter(lazy = true)
        private final FastSequencerLoader fastSequencerLoader = fslFactory();

        private CorfuRuntime runtimeFactory() {
            return CorfuRuntime.fromParameters(runtimeParamsBuilder.build());
        }

        private FastObjectLoader folFactory() {
            return fastObjectLoaderBuilder
                    .config(getConfig())
                    .objectsView(getRuntime().getObjectsView())
                    .addressSpaceLoader(getAddressSpaceLoader())
                    .customTypeStreams(getCustomTypeStreams())
                    .streamLoader(getStreamLoader())
                    .streamsMetaData(getStreamsMetaData())
                    .build();
        }

        private FastSequencerLoader fslFactory() {
            return fastSequencerLoaderBuilder
                    .addressSpaceLoader(getAddressSpaceLoader())
                    .build();
        }

        private AddressSpaceLoader addressSpaceLoaderFactory() {
            return addressLoaderBuilder
                    .addressSpaceView(getRuntime().getAddressSpaceView())
                    .build();
        }
    }
}
