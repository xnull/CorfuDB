package org.corfudb.universe.dynamic.events;

import com.spotify.docker.client.DockerClient;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.ContainerStats;
import org.corfudb.universe.dynamic.Dynamic;
import org.corfudb.universe.dynamic.PhaseState;
import org.corfudb.universe.dynamic.PhaseState.ServerPhaseState;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A event representing the intention of a client to put data from a
 * stream table in corfu.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public class PutDataEvent extends DataPathEvent {

    /**
     * Data generation function to use.
     */
    private CorfuTableDataGenerationFunction<String, String> generator;

    /**
     * Corfu client to use.
     */
    private Dynamic.CorfuClientInstance corfuClientInstance;

    /**
     * Get the name of the stream table where the data generarted will be saved in corfu.
     *
     * @return Name of the table stream in corfu.
     */
    public String getTableStreamName() {
        return this.generator.getTableStreamName();
    }

    public PutDataEvent(CorfuTableDataGenerationFunction<String, String> generator,
                        Dynamic.CorfuClientInstance corfuClientInstance,
                        DockerClient docker,
                        Collection<ServerPhaseState> servers) {
        super(docker, servers);
        this.generator = generator;
        this.corfuClientInstance = corfuClientInstance;
    }

    /**
     * Get a description of the observed change produced in the state of the universe
     * when this events happened.
     */
    @Override
    public String getObservationDescription() {
        return String.format("Put data using the %s generator", this.generator.getName());
    }

    /**
     * This method is called before {@link PutDataEvent::executeRealPartialTransition}.
     * The method must perform the updates directly over the {@link PutDataEvent::currentDesireState} reference.
     *
     * @param currentDesireState Desire-state of the universe before this event happened.
     * @return Desire-state of the universe after this event happened.
     */
    @Override
    public void applyDesirePartialTransition(PhaseState currentDesireState) {
        try{
            if(currentDesireState.getClusterStatus() != ClusterStatusReport.ClusterStatus.UNAVAILABLE) {
                currentDesireState.updateClientPutThroughput(this.corfuClientInstance.getName(),
                        this.corfuClientInstance.getLastMeasuredPutThroughput());
                this.generator.changeValues();
                currentDesireState.putDataToTableStream(this.generator.getTableStreamName(), this.generator);
            }else {
                currentDesireState.updateClientPutThroughput(this.corfuClientInstance.getName(), 0);
                currentDesireState.updateStatusOfTableStream(this.generator.getTableStreamName(),
                        PhaseState.TableStreamStatus.UNAVAILABLE);
            }

        }catch (CloneNotSupportedException cne){
            log.error("Unexpected cloning error while applying desire state for the Local Client Put Data Event.",
                    cne);
        }
    }

    /**
     * Execute the transition of the universe that materialize the occurrence of the event.
     * The method must perform the updates directly over the {@link PutDataEvent::currentRealState} reference.
     *
     * @param currentRealState Real-state of the universe before this event happened.
     */
    @Override
    public void executeRealPartialTransition(PhaseState currentRealState) {
        CorfuTable table = this.corfuClientInstance.getCorfuClient().
                createDefaultCorfuTable(this.generator.getTableStreamName());
        Map<String, String> dataToPut = this.generator.get();
        double throughput = 0;
        try{
            long ts1 = System.nanoTime();
            startCollectStats();
            for (Map.Entry<String, String> entry : dataToPut.entrySet()) {
                table.put(entry.getKey(), entry.getValue());
            }
            stopCollectStats();
            long ts2 = System.nanoTime();
            double td = (ts2 - ts1);
            throughput = dataToPut.size() / (td * 1.0) * 1000000;
            currentRealState.putDataToTableStream(this.generator.getTableStreamName(), this.generator);
        }catch (Exception ex){
            currentRealState.updateStatusOfTableStream(this.generator.getTableStreamName(),
                    PhaseState.TableStreamStatus.UNAVAILABLE);
            log.error(String.format("Unexpected error while putting Data for %s generatorSnapshot",
                    this.generator.getName()),
                    ex);
            this.generator.undoChangeValues();
        }
        this.corfuClientInstance.setLastMeasuredPutThroughput(throughput);
        currentRealState.updateClientPutThroughput(this.corfuClientInstance.getName(), throughput);
    }
}
