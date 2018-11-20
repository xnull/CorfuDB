package org.corfudb.recovery;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NonNull;

import java.time.Duration;

@Builder
@Getter
public class FastObjectLoaderConfig {

    private final boolean cacheDisabled;
    /**
     * How much time the Fast Loader has to get the maps up to date.
     *
     * <p>Once the timeout is reached, the Fast Loader gives up. Every map that is
     * not up to date will be loaded through normal path.
     *
     */
    @Default
    @NonNull
    private final Duration timeout = Duration.ofMinutes(30);

    private final boolean logHasNoCheckPoint;
}
