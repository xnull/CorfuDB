package org.corfudb.universe.dynamic;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import org.corfudb.universe.UniverseFactory;
import org.corfudb.universe.logging.LoggingParams;
import org.corfudb.universe.scenario.Scenario;
import org.corfudb.universe.scenario.fixture.Fixtures;
import org.corfudb.universe.universe.Universe;
import org.corfudb.universe.universe.UniverseParams;
import org.corfudb.universe.universe.vm.ApplianceManager;
import org.corfudb.universe.universe.vm.VmUniverseParams;

public abstract class UniverseInitializer {
    private static final UniverseFactory UNIVERSE_FACTORY = UniverseFactory.getInstance();

    private String testName;
    protected DockerClient docker;
    protected Universe universe;

    private final Universe.UniverseMode universeMode = Universe.UniverseMode.DOCKER;

    public void initialize() throws DockerCertificateException {
        docker = DefaultDockerClient.fromEnv().build();
    }

    public void shutdown() {
        if (universe != null) {
            universe.shutdown();
        }
    }

    public LoggingParams getDockerLoggingParams() {
        return LoggingParams.builder()
                .testName(this.testName)
                .enabled(false)
                .build();
    }

    public Scenario getVmScenario(int numNodes) {
        Fixtures.VmUniverseFixture universeFixture = new Fixtures.VmUniverseFixture();
        universeFixture.setNumNodes(numNodes);

        VmUniverseParams universeParams = universeFixture.data();

        ApplianceManager manager = ApplianceManager.builder()
                .universeParams(universeParams)
                .build();

        universe = UNIVERSE_FACTORY
                .buildVmUniverse(universeParams, manager)
                .deploy();

        return Scenario.with(universeFixture);
    }

    public Scenario getDockerScenario(int numNodes) {
        Fixtures.UniverseFixture universeFixture = new Fixtures.UniverseFixture();
        universeFixture.setNumNodes(numNodes);

        universe = UNIVERSE_FACTORY
                .buildDockerUniverse(universeFixture.data(), docker, getDockerLoggingParams())
                .deploy();

        return Scenario.with(universeFixture);
    }

    public Scenario<UniverseParams, Fixtures.AbstractUniverseFixture<UniverseParams>> getScenario() {
        final int defaultNumNodes = 3;
        return getScenario(defaultNumNodes);
    }

    public Scenario<UniverseParams, Fixtures.AbstractUniverseFixture<UniverseParams>> getScenario(int numNodes) {
        switch (universeMode) {
            case DOCKER:
                return getDockerScenario(numNodes);
            case VM:
                return getVmScenario(numNodes);
            case PROCESS:
                throw new UnsupportedOperationException("Not implemented");
            default:
                throw new UnsupportedOperationException("Not implemented");
        }
    }

    public UniverseInitializer(String testName) {
        this.testName = testName;
    }
}
