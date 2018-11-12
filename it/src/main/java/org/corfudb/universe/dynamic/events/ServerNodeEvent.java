package org.corfudb.universe.dynamic.events;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.PhaseState;
import org.corfudb.universe.node.server.CorfuServer;

import java.time.Duration;

/**
 * A event representing a action over a corfu server node.
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public abstract class ServerNodeEvent implements UniverseEvent {

    /**
     * Name of the corfu node server associated to the event.
     */
    @Getter
    protected final String nodeName;

    /**
     * Corfu server associated to the event.
     */
    protected final CorfuServer corfuServer;

    /**
     * A short description of the action over a corfu server node.
     *
     * @return Short description of the action.
     */
    protected abstract String getActionDescription();

    /**
     * Get a description of the observed change produced in the state of the universe
     * when this events happened.
     */
    @Override
    public String getObservationDescription() {
        return String.format("%s corfu server node %s", this.getActionDescription(), this.nodeName);
    }

    public ServerNodeEvent(String nodeName, CorfuServer corfuServer) {
        this.nodeName = nodeName;
        this.corfuServer = corfuServer;
    }

    /**
     * Stop a corfu server node.
     */
    public static class Stop extends ServerNodeEvent {

        /**
         * Duration passed to the stop action.
         */
        private final Duration duration;

        /**
         * A short description of the action over a corfu server node.
         *
         * @return Short description of the action.
         */
        @Override
        protected String getActionDescription() {
            return "Stop";
        }

        /**
         * Get the desire-state of the universe after this event happened.
         * This method is called before {@link UniverseEventOperator::executeRealPartialTransition}.
         * The method must perform the updates directly over the currentDesireState reference.
         *
         * @param currentDesireState Desire-state of the universe before this event happened.
         * @return Desire-state of the universe after this event happened.
         */
        @Override
        public void applyDesirePartialTransition(PhaseState currentDesireState) {
            currentDesireState.updateServerStatus(this.nodeName, ClusterStatusReport.NodeStatus.DOWN);
            if(currentDesireState.getUnresponsiveServers().size() == currentDesireState.getServers().size()){
                currentDesireState.setClusterStatus(ClusterStatusReport.ClusterStatus.UNAVAILABLE);
            }else{
                currentDesireState.setClusterStatus(ClusterStatusReport.ClusterStatus.DEGRADED);
            }
        }

        /**
         * Execute the transition of the universe that materialize the occurrence of the event.
         * The method must perform the updates directly over the parameter currentRealState reference.
         *
         * @param currentRealState Real-state of the universe before this event happened.
         */
        @Override
        public void executeRealPartialTransition(PhaseState currentRealState) {
            this.corfuServer.stop(this.duration);
        }

        public Stop(String nodeName, CorfuServer corfuServer, Duration duration){
            super(nodeName, corfuServer);
            this.duration = duration;
        }
    }

    /**
     * Start a corfu server node.
     */
    public static class Start extends ServerNodeEvent {

        /**
         * A short description of the action over a corfu server node.
         *
         * @return Short description of the action.
         */
        @Override
        protected String getActionDescription() {
            return "Start";
        }

        /**
         * Get the desire-state of the universe after this event happened.
         * This method is called before @executeRealPartialTransition.
         * The method must perform the updates directly over the @currentDesireState reference.
         *
         * @param currentDesireState Desire-state of the universe before this event happened.
         * @return Desire-state of the universe after this event happened.
         */
        @Override
        public void applyDesirePartialTransition(PhaseState currentDesireState) {
            currentDesireState.updateServerStatus(this.nodeName, ClusterStatusReport.NodeStatus.UP);
            if(currentDesireState.getUnresponsiveServers().size() == 0){
                currentDesireState.setClusterStatus(ClusterStatusReport.ClusterStatus.STABLE);
            }else{
                currentDesireState.setClusterStatus(ClusterStatusReport.ClusterStatus.DEGRADED);
            }
        }

        /**
         * Execute the transition of the universe that materialize the occurrence of the event.
         * The method must perform the updates directly over the parameter currentRealState reference.
         *
         * @param currentRealState Real-state of the universe before this event happened.
         */
        @Override
        public void executeRealPartialTransition(PhaseState currentRealState) {
            this.corfuServer.start();
        }

        public Start(String nodeName, CorfuServer corfuServer){
            super(nodeName, corfuServer);
        }
    }
}
