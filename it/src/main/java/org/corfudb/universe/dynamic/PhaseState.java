package org.corfudb.universe.dynamic;

import lombok.Data;
import lombok.Getter;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.events.CorfuTableDataGenerationFunction;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents the state of the universe in a point of time.
 *
 * The phase state is the abstraction that encapsulate all the parameters that describe
 * the evolution of the universe as a whole.
 * This abstraction does not need to consider any parameter used for the creation of
 * the universe that does not change during its life, e.g.
 *      - name of the network for the nodes.
 *      - type of nodes (VMs or Containers).
 *      - etc.
 * Parameters of the universe that change during its life but that are irrelevant for the
 * evaluation of the behaviour or performance of the system as whole, must be keep it
 * out of this abstraction.
 * The only parameters that we need to track in the state are those that can be compared
 * against themselves in order to produce a metric that allow us to evaluate the behaviour
 * or performance of the system as whole.
 *
 * Created by edilmo on 11/06/18.
 */
@Data
public class PhaseState implements Cloneable {

    /**
     * Cluster health status.
     */
    private ClusterStatusReport.ClusterStatus clusterStatus = ClusterStatusReport.ClusterStatus.STABLE;

    /**
     * A list of servers in the universe.
     */
    private final Map<String, ServerPhaseState> servers = new HashMap<>();

    /**
     * A list of clients in the universe.
     */
    private final Map<String, ClientPhaseState> clients = new HashMap<>();

    /**
     * A map which keep track of the data put in each stream.
     * In order to have a efficient representation of the data that allow to test
     * with a payload arbitrary large, no data at all is saved, just
     * a instance of a class that describe how to produce the data.
     *
     * The key of the map is the name of the stream in corfu.
     * The value of the map is a special state class that includes
     * the a status of the stream as whole and a sequence of values put in the stream
     * during the time. The idea of this is to give support to the
     * snapshot feature of corfu.
     */
    private final Map<String, TableStreamPhaseState> data = new HashMap<>();

    public Object clone() {
        PhaseState clone = new PhaseState();
        clone.clusterStatus = this.clusterStatus;
        for(Map.Entry<String, ServerPhaseState> entry: this.servers.entrySet()){
            clone.servers.put(entry.getKey(), (ServerPhaseState)entry.getValue().clone());
        }
        for(Map.Entry<String, ClientPhaseState> entry: this.clients.entrySet()){
            clone.clients.put(entry.getKey(), (ClientPhaseState)entry.getValue().clone());
        }
        for(Map.Entry<String, TableStreamPhaseState> entry: this.data.entrySet()){
            clone.data.put(entry.getKey(), (TableStreamPhaseState)entry.getValue().clone());
        }
        return clone;
    }

    /**
     * Update the status of the given server.
     *
     * @param nodeName  Name of the corfu server node.
     * @param status    New status of the corfu server node.
     */
    public void updateServerStatus(String nodeName, ClusterStatusReport.NodeStatus status) {
        this.servers.get(nodeName).setStatus(status);
    }

    /**
     * Put the generator of a new data in the phase state of the table stream.
     *
     * @param tableStreamName           Name of the table stream used.
     * @param corfuTableDataGenerationFunction    Data generator used.
     * @throws CloneNotSupportedException
     */
    public synchronized void putDataToTableStream(String tableStreamName, CorfuTableDataGenerationFunction corfuTableDataGenerationFunction)
            throws CloneNotSupportedException {
        if(this.data.containsKey(tableStreamName)){
            this.data.get(tableStreamName).getDataTableStream().add(corfuTableDataGenerationFunction.getSnapshot());
        }
        else{
            TableStreamPhaseState tableStreamPhaseState = new TableStreamPhaseState(tableStreamName);
            tableStreamPhaseState.getDataTableStream().add(corfuTableDataGenerationFunction.getSnapshot());
            this.data.put(tableStreamName, tableStreamPhaseState);
        }
    }

    /**
     * Whether data has been put in the specified table stream or not.
     *
     * @param tableStreamName   Name of the table stream.
     * @return                  Whether data has been put in the specified table stream or not.
     */
    public boolean hasDataInTableStream(String tableStreamName) {
        return this.data.containsKey(tableStreamName) && !this.data.get(tableStreamName).dataTableStream.isEmpty();
    }

    /**
     * Update the status of a corfu table stream.
     *
     * @param tableStreamName   Name of the corfu table stream.
     * @param status            Status of the corfu table stream.
     */
    public synchronized void updateStatusOfTableStream(String tableStreamName, TableStreamStatus status) {
        if(this.data.containsKey(tableStreamName)){
            this.data.get(tableStreamName).setStatus(status);
        }
    }

    /**
     * Update the put throughput for a given client.
     *
     * @param clientName name of the client to update.
     * @param throughput throughput value to update.
     */
    public synchronized void updateClientPutThroughput(String clientName, double throughput) {
        this.clients.get(clientName).setPutThroughput(throughput);
    }

    /**
     * Update the put throughput for a given client.
     *
     * @param clientName name of the client to update.
     * @param throughput throughput value to update.
     */
    public synchronized void updateClientGetThroughput(String clientName, double throughput) {
        this.clients.get(clientName).setGetThroughput(throughput);
    }

    /**
     * This function returns a set of all servers in the universe.
     *
     * @return A set of all servers in the universe.
     */
    public Set<String> getAllServers() {
        return new HashSet<String>(servers.values().stream().map(ps -> ps.nodeName).collect(Collectors.toList()));
    }

    /**
     * This function returns a set of unresponsive servers in the universe.
     *
     * @return A set of unresponsive servers in the universe.
     */
    public Set<String> getUnresponsiveServers() {
        return new HashSet<String>(servers.values().stream().
                filter(ps -> ps.status == ClusterStatusReport.NodeStatus.DOWN).
                map(ps -> ps.nodeName).collect(Collectors.toList()));
    }

    /**
     * Computes the Average putThroughput of all clients.
     *
     * @return Average putThroughput of all clients or zero if the average couldn't be computed
     */
    public double getAveragePutThroughput() {
        return clients.values().stream().mapToDouble(c -> c.putThroughput).average().orElse(0);
    }

    /**
     * Computes the Average getThroughput of all clients.
     *
     * @return Average getThroughput of all clients or zero if the average couldn't be computed
     */
    public double getAverageGetThroughput() {
        return clients.values().stream().mapToDouble(c -> c.getThroughput).average().orElse(0);
    }

    /**
     * Computes the count of corfu table streams for each possible status possible.
     *
     * @return Map with {@link TableStreamStatus} as key and the count of corfu tables in that status as value.
     */
    public Map<TableStreamStatus, Long> getTableStreamsStatusAggregations() {
        Map<TableStreamStatus, Long> aggregation = this.data.values().stream().collect(
                Collectors.groupingBy(TableStreamPhaseState::getStatus, Collectors.counting()));
        if(!aggregation.containsKey(TableStreamStatus.CORRECT))
            aggregation.put(TableStreamStatus.CORRECT, 0L);
        if(!aggregation.containsKey(TableStreamStatus.PARTIALLY_CORRECT))
            aggregation.put(TableStreamStatus.PARTIALLY_CORRECT, 0L);
        if(!aggregation.containsKey(TableStreamStatus.INCORRECT))
            aggregation.put(TableStreamStatus.INCORRECT, 0L);
        if(!aggregation.containsKey(TableStreamStatus.UNAVAILABLE))
            aggregation.put(TableStreamStatus.UNAVAILABLE, 0L);
        return aggregation;
    }

    public static final String SUMMARY_FIELD_CLUSTER_STATUS = "Cluster";
    public static final String SUMMARY_FIELD_SERVERS_TOTAL = "Servers";
    public static final String SUMMARY_FIELD_SERVERS_DOWN = "Servers Down";
    public static final String SUMMARY_FIELD_AVERAGE_PUT_THROUGHPUT = "Average Put Throughput";
    public static final String SUMMARY_FIELD_AVERAGE_GET_THROUGHPUT = "Average Get Throughput";
    public static final String SUMMARY_FIELD_UNAVAILABLE_TABLES = " Tables Unavailable";
    public static final String SUMMARY_FIELD_INCORRECT_TABLES = " Tables Incorrect";
    public static final String SUMMARY_FIELD_PARTIALLY_CORRECT_TABLES = " Tables Partially Correct";
    public static final String SUMMARY_FIELD_CORRECT_TABLES = " Tables Correct";

    /**
     * Summary of the state in form of a map field->value.
     *
     * @return  Summary table of the state.
     */
    public Map<String, String> getSummary(String prefix) {
        if(!prefix.isEmpty())
            prefix += " ";
        Map<String, String> summary = new HashMap<>();
        summary.put(prefix +SUMMARY_FIELD_CLUSTER_STATUS, this.clusterStatus.name());
        summary.put(prefix +SUMMARY_FIELD_SERVERS_TOTAL, Integer.toString(this.servers.size()));
        summary.put(prefix +SUMMARY_FIELD_SERVERS_DOWN, Integer.toString(this.getUnresponsiveServers().size()));
        summary.put(prefix +SUMMARY_FIELD_AVERAGE_PUT_THROUGHPUT, Double.toString(this.getAveragePutThroughput()));
        summary.put(prefix +SUMMARY_FIELD_AVERAGE_GET_THROUGHPUT, Double.toString(this.getAverageGetThroughput()));
        Map<TableStreamStatus, Long> tableStatus = this.getTableStreamsStatusAggregations();
        summary.put(prefix +SUMMARY_FIELD_UNAVAILABLE_TABLES, Long.toString(tableStatus.get(TableStreamStatus.UNAVAILABLE)));
        summary.put(prefix +SUMMARY_FIELD_INCORRECT_TABLES, Long.toString(tableStatus.get(TableStreamStatus.INCORRECT)));
        summary.put(prefix +SUMMARY_FIELD_PARTIALLY_CORRECT_TABLES, Long.toString(tableStatus.get(TableStreamStatus.PARTIALLY_CORRECT)));
        summary.put(prefix +SUMMARY_FIELD_CORRECT_TABLES, Long.toString(tableStatus.get(TableStreamStatus.CORRECT)));
        return summary;
    }

    /**
     * Represents the state of a node server in a point of time.
     */
    @Data
    public static class ServerPhaseState implements Cloneable {

        /**
         * Name of the node server
         */
        private final String nodeName;

        /**
         * Endpoint of the node server
         */
        private final String nodeEndpoint;


        /**
         * Connectivity to the node.
         */
        private ClusterStatusReport.NodeStatus status = ClusterStatusReport.NodeStatus.UP;

        public Object clone(){
            ServerPhaseState clone = new ServerPhaseState(this.nodeName, this.nodeEndpoint);
            clone.status = this.status;
            return clone;
        }
    }

    /**
     * Represents the state of a node server in a point of time.
     */
    @Data
    public static class ClientPhaseState implements Cloneable {

        /**
         * Name of the node client
         */
        private final String clientName;

        /**
         * Throughput that the client see when is writing data to corfu.
         */
        private double putThroughput = 0;

        /**
         * Throughput that the client see when is getting data from corfu.
         */
        private double getThroughput = 0;

        public Object clone(){
            ClientPhaseState clone = new ClientPhaseState(this.clientName);
            clone.putThroughput = this.putThroughput;
            clone.getThroughput = this.getThroughput;
            return clone;
        }
    }

    /**
     * Represents the state of a table stream in corfu.
     *
     * The state includes all the data put in the table stream during
     * the whole history.
     * In order to have a efficient representation of the data that allows to test
     * with a payload arbitrary large, no data at all is saved, just
     * a instance of a class that describe how to produce the data.
     */
    @Data
    public static class TableStreamPhaseState implements Cloneable {

        /**
         * Name of the table stream in corfu.
         */
        private final String tableStreamName;

        /**
         * Concrete status of the stream after a GetData event.
         * The idea is simple:
         *      - Put data: always forced this status to CORRECT.
         *      - Get data: mark the status as INCORRECT if and only if
         *                  the retrieved data is not equal to the data
         *                  in this state.
         *      - Get data snapshot: mark the status as PARTIALLY_CORRECT if and only if
         *                           the retrieved snapshot data is not equal to
         *                           the data in this state.
         *      - All: mark the status as UNAVAILABLE if the operation failed.
         */
        private TableStreamStatus status = TableStreamStatus.CORRECT;

        /**
         * Sequence of values put in the stream during the time.
         * The idea of this is to give support to the snapshot feature of corfu.
         */
        public final List<CorfuTableDataGenerationFunction> dataTableStream = new ArrayList<>();

        public Object clone() {
            TableStreamPhaseState clone = new TableStreamPhaseState(this.tableStreamName);
            clone.status = this.status;
            clone.dataTableStream.addAll(this.dataTableStream);
            return clone;
        }
    }

    /**
     * Possible status of a table stream in corfu.
     */
    public enum TableStreamStatus {
        /**
         * The data retrieved from corfu is equal to the data expected.
         */
        CORRECT(0),

        /**
         * The data retrieved from corfu for one snapshot is not equal to the data expected.
         */
        PARTIALLY_CORRECT(1),

        /**
         * The data retrieved from corfu is not equal to the data expected.
         */
        INCORRECT(2),

        /**
         * The data is not available.
         */
        UNAVAILABLE(3);

        @Getter
        final int statusValue;

        TableStreamStatus(int statusValue) {
            this.statusValue = statusValue;
        }
    }
}
