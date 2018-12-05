package org.corfudb.universe.dynamic;

import com.spotify.docker.client.exceptions.DockerCertificateException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.events.*;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.ClientParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.CorfuServer;
import org.corfudb.util.Sleep;

import java.time.Duration;
import java.util.*;
import java.util.function.Supplier;

/**
 * Simulates the behavior of a corfu cluster and its clients during a period of time given.
 * General speaking, any dynamical system (including finite state machines) can be approximated with:
 *      - Events
 *      - Operator that combine events to produce complex events
 *      - Rules over the events and its combinations
 *      - State that describe the system as whole
 *      - Dynamic that generate the events (or combinations of them), ensure the rules and
 *      produce transitions over the state based in the events.
 * This is the approach follow here, the method {@link Dynamic::run} implements a event loop with
 * logic described above.
 */
@Slf4j
public abstract class Dynamic extends UniverseInitializer {

    /**
     * Amount of corfu tables handle for each client in the universe.
     */
    private static final int DEFAULT_AMOUNT_OF_CORFU_CLIENTS = 3;

    /**
     * Random number generator used for the creation of
     * random intervals of time and generation of events.
     */
    protected final Random randomNumberGenerator = new Random(210580);

    /**
     * The cluster of corfu nodes to use.
     */
    protected CorfuCluster corfuCluster;

    /**
     * Amount of nodes defined in the cluster.
     */
    protected int numNodes;

    /**
     * Fixture of parameters used in the creation of clients.
     */
    protected ClientParams clientFixture;

    /**
     * The max amount of milli seconds that the dynamic can live.
     */
    protected final long longevity;

    /**
     * Clients used during the life of the dynamic.
     *
     * Clients are created during the initialization and keep it fixed up to the end.
     */
    protected final List<CorfuClientInstance> corfuClients = new ArrayList<>();

    /**
     * Corfu nodes present in the cluster.
     */
    protected final LinkedHashMap<String, CorfuServer> corfuServers = new LinkedHashMap<>();

    /**
     * Represents the expected state of the universe in a point of time.
     */
    protected PhaseState desireState = new PhaseState();

    /**
     * Represents the real state of the universe in a point of time.
     */
    protected final PhaseState realState = new PhaseState();

    /**s
     * Time in milliseconds before update the state.
     */
    protected abstract long getDelayForStateUpdate();

    /**s
     * Time in milliseconds between the composite event that just happened and the next one.
     */
    protected abstract long getIntervalBetweenEvents();

    /**
     * Generate a composite event to materialize in the universe.
     * The events generated have not guaranty of execution. They are allowed
     * to execute if they are compliant with the {@link UniverseRule}.
     *
     * @return  Composite event generated.
     */
    protected abstract UniverseEventOperator generateCompositeEvent();

    /**
     * Materialize the execution of a composite event if it is compliant with the {@link UniverseRule}.
     *
     * @param compositeEvent    Composite event to materialize.
     * @return                  Whether the composite event was materialized or not.
     */
    private boolean materializeCompositeEvent(UniverseEventOperator compositeEvent) {
        boolean isPreRulesCompliant = true;
        for(UniverseRule preRule: UniverseRule.getPreCompositeEventRules()){
            if(!preRule.check(compositeEvent, this.desireState, this.realState)){
                isPreRulesCompliant = false;
                break;
            }
        }
        if(!isPreRulesCompliant)
            return false;
        //The next desire state is based in changes over the current real state
        PhaseState desireStateCopy = (PhaseState)this.realState.clone();
        compositeEvent.applyDesirePartialTransition(desireStateCopy);
        boolean isPostRulesCompliant = true;
        for(UniverseRule postRule: UniverseRule.getPostCompositeEventRules()){
            if(!postRule.check(compositeEvent, desireStateCopy, this.realState)){
                isPostRulesCompliant = false;
                break;
            }
        }
        if(!isPostRulesCompliant)
            return false;
        this.desireState = desireStateCopy;
        compositeEvent.executeRealPartialTransition(this.realState);
        this.updateRealState();
        return true;
    }

    /**
     * Updates the real state using the tools provided by the universe.
     */
    private void updateRealState() {
        try {
            Sleep.MILLISECONDS.sleepUninterruptibly(this.getDelayForStateUpdate());
        }
        catch (Exception ex){
            log.error("Error invoking Thread.sleep." , ex);
        }
        ClusterStatusReport clusterStatusReport = this.corfuClients.get(0).corfuClient.
                getManagementView().getClusterStatus();
        this.realState.setClusterStatus(clusterStatusReport.getClusterStatus());
        Map<String, ClusterStatusReport.NodeStatus> statusMap = clusterStatusReport.getClientServerConnectivityStatusMap();
        for(PhaseState.ServerPhaseState serverState: this.realState.getServers().values()){
            if(statusMap.containsKey(serverState.getNodeEndpoint()))
                serverState.setStatus(statusMap.get(serverState.getNodeEndpoint()));
            else
                log.error(String.format("Expected server not found in cluster status report. Node Name: %s - Node Endpoint: %s",
                        serverState.getNodeName(), serverState.getNodeEndpoint()));
        }
    }

    public static final String REPORT_FIELD_EVENT = "Event";
    public static final String REPORT_FIELD_DISCARDED_EVENTS = "Discarded Events";

    /**
     * Report the the desire state and real state after each iteration of the event generation loop.
     *
     * @param compositeEvent    Composite event that change the states.
     */
    private void reportStateDifference(UniverseEventOperator compositeEvent, int discardedEvents) {
        Map<String, String> report = new HashMap<>();
        String eventDescription = compositeEvent.getObservationDescription();
        log.info(eventDescription);
        report.put(REPORT_FIELD_EVENT, eventDescription);
        log.info("Real state summary:");
        Map<String, String> summaryRealState = this.realState.getSummary("Real");
        for(Map.Entry<String, String> entry: summaryRealState.entrySet()){
            log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        report.putAll(summaryRealState);
        log.info("Desire state summary:");
        Map<String, String> summaryDesireState = this.desireState.getSummary("Desire");
        for(Map.Entry<String, String> entry: summaryDesireState.entrySet()){
            log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
        }
        report.putAll(summaryDesireState);
        log.info(String.format("%s: %s", REPORT_FIELD_DISCARDED_EVENTS, discardedEvents));
        report.put(REPORT_FIELD_DISCARDED_EVENTS, Integer.toString(discardedEvents));
        //TODO: send report to a spreadsheet
    }

    /**
     * Run the dynamic defined during the time specified, using a event loop.
     *
     */
    public void run() {
        getScenario().describe((fixture, testCase) -> {
            this.numNodes = fixture.getNumNodes();
            this.clientFixture = fixture.getClient();
            this.corfuCluster = universe.getGroup(fixture.getCorfuCluster().getName());
            for(int i = 0; i < DEFAULT_AMOUNT_OF_CORFU_CLIENTS; i++){
                CorfuClient corfuClient = corfuCluster.getLocalCorfuClient();
                String clientName = corfuClient.getParams().getName() + i;
                CorfuClientInstance corfuClientInstance = new CorfuClientInstance(i, clientName, corfuClient);
                this.corfuClients.add(corfuClientInstance);
                PhaseState.ClientPhaseState desireClientState = new PhaseState.ClientPhaseState(clientName);
                this.desireState.getClients().put(clientName, desireClientState);
                PhaseState.ClientPhaseState realClientState = new PhaseState.ClientPhaseState(clientName);
                this.realState.getClients().put(clientName, realClientState);
                for(CorfuTableDataGenerationFunction corfuTableDataGenerator: corfuClientInstance.corfuTables){
                    PhaseState.TableStreamPhaseState desireTableState =
                            new PhaseState.TableStreamPhaseState(corfuTableDataGenerator.getTableStreamName());
                    this.desireState.getData().put(desireTableState.getTableStreamName(), desireTableState);
                    PhaseState.TableStreamPhaseState realTableState =
                            new PhaseState.TableStreamPhaseState(corfuTableDataGenerator.getTableStreamName());
                    this.realState.getData().put(desireTableState.getTableStreamName(), realTableState);
                }
            }
            for(int i = 0; i < this.numNodes; i ++){
                String nodeName = "node" + (9000 + i);
                CorfuServer nodeServer = (CorfuServer)corfuCluster.getNode(nodeName);
                this.corfuServers.put(nodeName, nodeServer);
                PhaseState.ServerPhaseState desireServerState = new PhaseState.ServerPhaseState(nodeName, nodeServer.getEndpoint());
                this.desireState.getServers().put(nodeName, desireServerState);
                PhaseState.ServerPhaseState realServerState = new PhaseState.ServerPhaseState(nodeName, nodeServer.getEndpoint());
                this.realState.getServers().put(nodeName, realServerState);
            }
            int discardedEvents = 0;
            long startTime = System.currentTimeMillis();
            while ( (System.currentTimeMillis() - startTime) < this.longevity){
                UniverseEventOperator compositeEvent = this.generateCompositeEvent();
                if(this.materializeCompositeEvent(compositeEvent)){
                    this.reportStateDifference(compositeEvent, discardedEvents);
                    discardedEvents = 0;
                    try {
                        Sleep.MILLISECONDS.sleepUninterruptibly(this.getIntervalBetweenEvents());
                    }
                    catch (Exception ex){
                        log.error("Error invoking Thread.sleep." , ex);
                    }
                }else{
                    discardedEvents++;
                }
            }
        });
    }

    /**
     * Initialize the universe and the dynamic.
     * @throws DockerCertificateException
     */
    @Override
    public void initialize() throws DockerCertificateException {
        super.initialize();
    }

    /**
     * Free resources used by the universe and the dynamic.
     */
    @Override
    public void shutdown() {
        super.shutdown();
    }

    public Dynamic(long longevity) {
        super("LongevityTest");
        this.longevity = longevity;
    }

    /**
     * Describe and holds a specific instance of a corfu client implementation
     * that is used in a {@link Dynamic}.
     */
    @Getter
    @Setter
    public class CorfuClientInstance {
        /**
         * Amount of corfu tables handle for each client in the universe.
         */
        private static final int DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT = 3;

        /**
         * Amount of corfu tables handle for each client in the universe.
         */
        private static final int DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE = 50;

        /**
         * Integer id that is used as a simple seed to make each data generation function different
         * in a deterministic a reproducible way.
         */
        protected int id;

        /**
         * Name of the client, it must be unique.
         */
        private final String name;

        /**
         * Throughput that the client see when is writing data to corfu.
         */
        private double lastMeasuredPutThroughput = 0;

        /**
         * Throughput that the client see when is getting data from corfu.
         */
        private double lastMeasuredGetThroughput = 0;

        /**
         * Data generators for each corfu table handle by this client.
         */
        private final List<CorfuTableDataGenerationFunction> corfuTables = new ArrayList<>();

        /**
         * Corfu client implementation to use.
         */
        private final CorfuClient corfuClient;

        public CorfuClientInstance(int id, String name, CorfuClient corfuClient){
            this.id = id;
            this.name = name;
            this.corfuClient = corfuClient;
            for(int i = 0; i < DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT; i++){
                int generatorId = (this.id * DEFAULT_AMOUNT_OF_CORFU_TABLES_PER_CLIENT) + i;
                if((i%2) == 0){
                    this.corfuTables.add(new CorfuTableDataGenerationFunction.IntegerSequence(generatorId,
                            DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE));
                }
                else{
                    this.corfuTables.add(new CorfuTableDataGenerationFunction.SinusoidalStrings(generatorId,
                            DEFAULT_AMOUNT_OF_FIELDS_PER_CORFU_TABLE));
                }
            }
        }
    }

    /**
     * Dynamic that sequentially alternate between PutData and GetData events, both over
     * a different stream table each time. Intended for basic troubleshooting.
     */
    public static class SimplePutGetDataDynamic extends Dynamic {

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

        /**
         * Whether the next event to generate is a put data event or not.
         */
        private boolean generatePut = true;

        /**
         * Index of the client to use for the next put data event to generate.
         */
        private int clientIndexForPutData = 0;

        /**
         * Index table to use for the next put data event to generate.
         */
        private int tableIndexForPutData = 0;

        /**
         * Generate a put data event using a different corfu table each time that is invoked.
         * @return
         */
        protected PutDataEvent generatePutDataEvent() {
            PutDataEvent event = new PutDataEvent(this.corfuClients.get(clientIndexForPutData).corfuTables.get(tableIndexForPutData),
                    this.corfuClients.get(clientIndexForPutData));
            tableIndexForPutData++;
            if(tableIndexForPutData >= this.corfuClients.get(clientIndexForPutData).corfuTables.size()){
                tableIndexForPutData = 0;
                clientIndexForPutData++;
                if(clientIndexForPutData >= this.corfuClients.size()){
                    clientIndexForPutData = 0;
                }
            }
            return event;
        }

        /**
         * Index of the client to use for the next get data event to generate.
         */
        private int clientIndexForGetData = 0;

        /**
         * Index table to use for the next get data event to generate.
         */
        private int tableIndexForGetData = 0;

        /**
         * Generate a get data event using a different corfu table each time that is invoked.
         * @return
         */
        protected GetDataEvent generateGetDataEvent() {
            GetDataEvent event = new GetDataEvent(this.corfuClients.get(clientIndexForGetData).corfuTables.get(tableIndexForGetData),
                    this.corfuClients.get(clientIndexForGetData));
            tableIndexForGetData++;
            if(tableIndexForGetData >= this.corfuClients.get(clientIndexForGetData).corfuTables.size()){
                tableIndexForGetData = 0;
                clientIndexForGetData++;
                if(clientIndexForGetData >= this.corfuClients.size()){
                    clientIndexForGetData = 0;
                }
            }
            return event;
        }

        /**
         * Time in milliseconds before update the state.
         */
        @Override
        protected long getDelayForStateUpdate() {
            return DEFAULT_DELAY_FOR_STATE_UPDATE;
        }

        /**
         * Time in milliseconds between the composite event that just happened and the next one.
         */
        @Override
        protected long getIntervalBetweenEvents() {
            return (long)(DEFAULT_INTERVAL_BETWEEN_EVENTS * this.randomNumberGenerator.nextDouble());
        }

        /**
         * Generate a composite event to materialize in the universe.
         * The events generated have not guaranty of execution. They are allowed
         * to execute if they are compliant with the {@link UniverseRule}.
         *
         * @return Composite event generated.
         */
        @Override
        protected UniverseEventOperator generateCompositeEvent() {
            UniverseEvent event = generatePut ? this.generatePutDataEvent() : this.generateGetDataEvent();
            generatePut = !generatePut;
            return new UniverseEventOperator.Single(event);
        }

        public SimplePutGetDataDynamic(long longevity){
            super(longevity);
        }
    }


    /**
     * Dynamic that sequentially alternate between Stop Server and Start Server events, with
     * PutData and GetData events between them. In each iteration, a different server is
     * stop/start. Intended for basic troubleshooting.
     */
    public static class SimpleStopStartDynamic extends SimplePutGetDataDynamic {

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

        /**
         * Duration for stop server events generated.
         */
        private static Duration DEFAULT_STOP_NODE_DURATION = Duration.ofSeconds(10);

        /**
         * Whether the next event to generate is a stop event or not.
         */
        private boolean generateStop = true;


        /**
         * Index of the server over which execute the next action.
         */
        private int serverIndex = 0;

        /**
         * Generate a put data event using a different corfu table each time that is invoked.
         * @return
         */
        protected ServerNodeEvent generateServerNodeEvent() {
            Set<Map.Entry<String, CorfuServer>> mapSet = this.corfuServers.entrySet();
            Map.Entry<String, CorfuServer> server = (Map.Entry<String, CorfuServer>)mapSet.toArray()[serverIndex];
            ServerNodeEvent event;
            if(generateStop){
                event = new ServerNodeEvent.Stop(server.getKey(), server.getValue(), DEFAULT_STOP_NODE_DURATION);
            }else{
                event = new ServerNodeEvent.Start(server.getKey(), server.getValue());
                serverIndex++;
                if(serverIndex >= this.corfuServers.size()){
                    serverIndex = 0;
                }
            }
            generateStop = !generateStop;
            return event;
        }

        /**
         * Time in milliseconds before update the state.
         */
        @Override
        protected long getDelayForStateUpdate() {
            return DEFAULT_DELAY_FOR_STATE_UPDATE;
        }

        /**
         * Time in milliseconds between the composite event that just happened and the next one.
         */
        @Override
        protected long getIntervalBetweenEvents() {
            return (long)(DEFAULT_INTERVAL_BETWEEN_EVENTS * this.randomNumberGenerator.nextDouble());
        }

        /**
         * Amount of data events generated since the last server node event.
         */
        private int dataEventsGenerated = MAX_CONSECUTIVE_DATA_EVENTS;

        /**
         * Max amount of data events between server node events.
         */
        private static int MAX_CONSECUTIVE_DATA_EVENTS = 4;

        /**
         * Generate a composite event to materialize in the universe.
         * The events generated have not guaranty of execution. They are allowed
         * to execute if they are compliant with the {@link UniverseRule}.
         *
         * @return Composite event generated.
         */
        @Override
        protected UniverseEventOperator generateCompositeEvent() {
            UniverseEventOperator event;
            if(this.dataEventsGenerated >= MAX_CONSECUTIVE_DATA_EVENTS){
                event = new UniverseEventOperator.Single(this.generateServerNodeEvent());
                this.dataEventsGenerated = 0;
            }
            else{
                event = super.generateCompositeEvent();
                this.dataEventsGenerated++;
            }
            return event;
        }

        public SimpleStopStartDynamic(long longevity){
            super(longevity);
        }
    }

    /**
     * Dynamic that randomly generate composite events
     */
    public static class PseudoRandomDynamic extends SimpleStopStartDynamic {

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_DELAY_FOR_STATE_UPDATE = 500;

        /**
         * Default time between two composite events in milliseconds.
         */
        private static int DEFAULT_INTERVAL_BETWEEN_EVENTS = 10000;

        /**
         * Time in milliseconds before update the state.
         */
        @Override
        protected long getDelayForStateUpdate() {
            return DEFAULT_DELAY_FOR_STATE_UPDATE;
        }

        /**
         * Time in milliseconds between the composite event that just happened and the next one.
         */
        @Override
        protected long getIntervalBetweenEvents() {
            return (long)(DEFAULT_INTERVAL_BETWEEN_EVENTS * this.randomNumberGenerator.nextDouble());
        }

        private final List<Supplier<UniverseEvent>> possibleEvents = new ArrayList<>();
        /**
         * Generate a composite event to materialize in the universe.
         * The events generated have not guaranty of execution. They are allowed
         * to execute if they are compliant with the {@link UniverseRule}.
         *
         * @return Composite event generated.
         */
        @Override
        protected UniverseEventOperator generateCompositeEvent() {
            List<UniverseEvent> actualEvents = new ArrayList<>();
            int i = 0;
            while ( i < this.possibleEvents.size() || actualEvents.isEmpty() ){
                if(this.randomNumberGenerator.nextBoolean()){
                    actualEvents.add(this.possibleEvents.get(i % 3).get());
                }
                i++;
            }
            UniverseEventOperator event = this.randomNumberGenerator.nextBoolean() ?
                    new UniverseEventOperator.Sequential(actualEvents) :
                    new UniverseEventOperator.Concurrent(actualEvents);
            return event;
        }

        public PseudoRandomDynamic(long longevity){
            super(longevity);
            this.possibleEvents.add(this::generatePutDataEvent);
            this.possibleEvents.add(this::generateGetDataEvent);
            this.possibleEvents.add(this::generateServerNodeEvent);
        }
    }
}
