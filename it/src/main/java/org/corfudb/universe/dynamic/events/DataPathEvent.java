package org.corfudb.universe.dynamic.events;

import com.spotify.docker.client.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.dynamic.ContainerStats;
import org.corfudb.universe.dynamic.PhaseState;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.universe.dynamic.PhaseState.*;

@Slf4j
public abstract class DataPathEvent implements UniverseEvent {

    private DockerClient docker;

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    private Collection<ServerPhaseState> servers;

    public DataPathEvent(DockerClient docker, Collection<ServerPhaseState> servers) {
        this.docker = docker;
        this.servers = servers;
    }

    public void startCollectStats() {
        try {
            executor.scheduleAtFixedRate(() -> {
                servers.forEach(s -> {
                    try {
                        long cpuUsage = docker.stats(s.getHostName()).cpuStats().cpuUsage().totalUsage();
                        s.updateStats(cpuUsage);
                    } catch (Exception e) {
                        log.warn("Filed to get serverStats for node: {}", s, e);
                    }
                });
            }, 0, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void stopCollectStats() {
        executor.shutdownNow();
    }
}
