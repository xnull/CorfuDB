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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A event representing the intention of a client to get data from a
 * stream table in corfu.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public class GetDataEvent extends DataPathEvent {

    /**
     * Snapshot of the data generation function used.
     */
    private CorfuTableDataGenerationFunction<String, String> generatorSnapshot;

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
        return this.generatorSnapshot.getTableStreamName();
    }

    public GetDataEvent(CorfuTableDataGenerationFunction<String, String> generator,
                        Dynamic.CorfuClientInstance corfuClientInstance,
                        DockerClient docker,
                        Collection<ServerPhaseState> servers) {
        super(docker, servers);
        try{
            this.generatorSnapshot = generator.getSnapshot();
        }catch (CloneNotSupportedException cne){
            log.error("Unexpected error getting a snapshot of the data generator while creating a Local Client Get Data Event.",
                    cne);
            this.generatorSnapshot = generator;
        }
        this.corfuClientInstance = corfuClientInstance;
    }

    /**
     * Get a description of the observed change produced in the state of the universe
     * when this events happened.
     */
    @Override
    public String getObservationDescription() {
        return String.format("Get data using the %s generatorSnapshot", this.generatorSnapshot.getName());
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
        if(currentDesireState.getClusterStatus() != ClusterStatusReport.ClusterStatus.UNAVAILABLE) {
            currentDesireState.updateStatusOfTableStream(this.generatorSnapshot.getTableStreamName(),
                    PhaseState.TableStreamStatus.CORRECT);
            currentDesireState.updateClientGetThroughput(this.corfuClientInstance.getName(),
                    this.corfuClientInstance.getLastMeasuredGetThroughput());
        }else {
            currentDesireState.updateStatusOfTableStream(this.generatorSnapshot.getTableStreamName(),
                    PhaseState.TableStreamStatus.UNAVAILABLE);
            currentDesireState.updateClientGetThroughput(this.corfuClientInstance.getName(), 0);
        }
    }

    /**
     * Execute the transition of the universe that materialize the occurrence of the event.
     * The method must perform the updates directly over the {@link GetDataEvent::currentRealState} reference.
     *
     * @param currentRealState Real-state of the universe before this event happened.
     */
    @Override
    public void executeRealPartialTransition(PhaseState currentRealState) {
        CorfuTable table = this.corfuClientInstance.getCorfuClient().
                createDefaultCorfuTable(this.generatorSnapshot.getTableStreamName());
        Map<String, String> dataToRead = this.generatorSnapshot.get();
        HashMap<String, Object> tableDataRead = new HashMap<>();
        double throughput = 0;
        try{
            long ts1 = System.nanoTime();
            startCollectStats();
            for (String fieldName : dataToRead.keySet()) {
                tableDataRead.put(fieldName, table.get(fieldName));
            }
            stopCollectStats();
            long ts2 = System.nanoTime();
            double td = (ts2 - ts1);
            throughput = dataToRead.size() / (td * 1.0) * 1000000;
        }catch (Exception ex){
            log.error(String.format("Unexpected error while getting Data for %s generatorSnapshot",
                    this.generatorSnapshot.getName()),
                    ex);
        }
        this.corfuClientInstance.setLastMeasuredGetThroughput(throughput);
        currentRealState.updateClientGetThroughput(this.corfuClientInstance.getName(), throughput);
        boolean dataInTableStreamIsTheExpected = true;
        for (Map.Entry<String, String> entry : dataToRead.entrySet()) {
            if(!tableDataRead.containsKey(entry.getKey()) || !tableDataRead.get(entry.getKey()).equals(entry.getValue())){
                dataInTableStreamIsTheExpected = false;
                break;
            }
        }
        if(throughput == 0){
            //throughput equal to zero means that we were unable to retrieve all the data from the table
            currentRealState.updateStatusOfTableStream(this.generatorSnapshot.getTableStreamName(),
                    PhaseState.TableStreamStatus.UNAVAILABLE);
        }else if(!dataInTableStreamIsTheExpected){
            currentRealState.updateStatusOfTableStream(this.generatorSnapshot.getTableStreamName(),
                    PhaseState.TableStreamStatus.INCORRECT);
        }else {
            currentRealState.updateStatusOfTableStream(this.generatorSnapshot.getTableStreamName(),
                    PhaseState.TableStreamStatus.CORRECT);
        }
    }
}
