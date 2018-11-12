package org.corfudb.universe.dynamic;

import jnr.ffi.annotations.In;
import org.corfudb.runtime.view.ClusterStatusReport;
import org.corfudb.universe.dynamic.events.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulate the implementation of a rule that governs the dynamic
 * of a universe.
 * This class offers two list of singletons:
 *      - Pre Rules: the rules that must be checked before the desire transition
 *                   of a event is executed.
 *      - Pro Rules: the rules that must be checked after the desire transition
 *                   of a event is executed.
 * The rules are intended to allow a {@link Dynamic} safely generate events that could
 * be inconsistent by definition but never executed. The relevance of this is decoupled
 * the development of a {@link UniverseEvent} from the development of a {@link Dynamic}.
 *
 * Created by edilmo on 11/06/18.
 */
public abstract class UniverseRule {

    /**
     * The list of rules that must be checked before the desire transition
     * of a event is executed.
     */
    private static List<UniverseRule> preCompositeEventRules = new ArrayList<>();

    /**
     * The list of rules that must be checked after the desire transition
     * of a event is executed.
     */
    private static List<UniverseRule> postCompositeEventRules = new ArrayList<>();

    /**
     * Get the list of rules that must be checked before the desire transition
     * of a event is executed.
     *
     * @return List of pre rules.
     */
    public static List<UniverseRule> getPreCompositeEventRules() {
        if(preCompositeEventRules.isEmpty()){
            preCompositeEventRules.add(new GetDataThatExistRule());
            preCompositeEventRules.add(new StartStopNodeRule());
        }
        return preCompositeEventRules;
    }

    /**
     * Get the list of rules that must be checked after the desire transition
     * of a event is executed.
     *
     * @return List of post rules.
     */
    public static List<UniverseRule> getPostCompositeEventRules() {
        if(postCompositeEventRules.isEmpty()){
            postCompositeEventRules.add(new AtLeastNServerUpRule(1));
        }
        return postCompositeEventRules;
    }

    /**
     * Check if the composite event, desire state and real state, are all compliant
     * with the rule defined.
     *
     * @param compositeEvent    Composite event that is happening.
     * @param desireState       Current desire state of the universe.
     * @param realState         Current real state of the universe.
     * @return                  Whether the composite event, desire state and real state,
     *                          are all compliant with the rule.
     */
    public abstract boolean check(UniverseEventOperator compositeEvent, PhaseState desireState, PhaseState realState);

    /**
     * Rule to ensure the minimum amount of nodes that should be up at any point of time.
     */
    private static class AtLeastNServerUpRule extends UniverseRule {
        /**
         * Minimum amount of nodes that should be up at any point of time.
         */
        private final int minNumNodes;

        /**
         * Check if the composite event, desire state and real state, are all compliant
         * with the rule defined.
         *
         * @param compositeEvent    Composite event that is happening.
         * @param desireState       Current desire state of the universe.
         * @param realState         Current real state of the universe.
         * @return                  Whether the composite event, desire state and real state,
         *                          are all compliant with the rule.
         */
        @Override
        public boolean check(UniverseEventOperator compositeEvent, PhaseState desireState, PhaseState realState) {
            boolean pass = true;
            if((desireState.getServers().size() - desireState.getUnresponsiveServers().size()) < this.minNumNodes)
                pass = false;
            return pass;
        }

        private AtLeastNServerUpRule(int minNumNodes){
            this.minNumNodes = minNumNodes;
        }
    }

    /**
     * Rule to ensure that:
     *      - A {@link GetDataEvent} is executed if and only if some data exist for
     *      its respective corfu table stream and is not being modified in this same composite event.
     *      - If the data does not exist previously, the rule check whether the current
     *      event is a sequential composite event that is creating the data itself before to read it.
     *      - If the data is being modified, the rule check whether the current
     *      event is a sequential composite event that is modifying the data after to read it.
     */
    private static class GetDataThatExistRule extends UniverseRule {

        /**
         * Check if the composite event, desire state and real state, are all compliant
         * with the rule defined.
         *
         * @param compositeEvent    Composite event that is happening.
         * @param desireState       Current desire state of the universe.
         * @param realState         Current real state of the universe.
         * @return                  Whether the composite event, desire state and real state,
         *                          are all compliant with the rule.
         */
        @Override
        public boolean check(UniverseEventOperator compositeEvent, PhaseState desireState, PhaseState realState) {
            boolean pass = true;
            //Lets check if there is a invalid get data (over a empty corfu table).
            List<UniverseEvent> events = compositeEvent.getSimpleEventsToCompose();
            HashMap<String, Integer> getDataEvents = new HashMap<>();
            HashMap<Integer, GetDataEvent> invalidGetDataEvents = new HashMap<>();
            for(int i = 0; i < events.size(); i++){
                UniverseEvent event = events.get(i);
                if(event instanceof GetDataEvent){
                    GetDataEvent getDataEvent = (GetDataEvent)event;
                    getDataEvents.put(getDataEvent.getTableStreamName(), i);
                    String tableStreamName = getDataEvent.getTableStreamName();
                    if(!desireState.hasDataInTableStream(tableStreamName)){
                        invalidGetDataEvents.put(i, getDataEvent);
                    }
                }
            }
            HashMap<Integer, PutDataEvent> putDataEvents = new HashMap<>();
            HashMap<String, Integer> invalidPutDataEvents = new HashMap<>();
            for(int i = 0; i < events.size(); i++){
                UniverseEvent event = events.get(i);
                if(event instanceof PutDataEvent){
                    PutDataEvent putDataEvent = (PutDataEvent)event;
                    putDataEvents.put(i, putDataEvent);
                    if(getDataEvents.containsKey(putDataEvent.getTableStreamName())){
                        invalidPutDataEvents.put(putDataEvent.getTableStreamName(), i);
                    }
                }
            }
            if((!invalidGetDataEvents.isEmpty() || !invalidPutDataEvents.isEmpty()) && compositeEvent instanceof UniverseEventOperator.Sequential){
                //lets check if the previous invalid gets are executed over data that is being put
                //by the composite event itself
                for(Map.Entry<Integer, GetDataEvent> getDataEntry: invalidGetDataEvents.entrySet()){
                    List<Map.Entry<Integer, PutDataEvent>> filteredPutDataEvents = putDataEvents.entrySet().stream().
                            filter(pde -> pde.getValue().getTableStreamName().equals(getDataEntry.getValue().getTableStreamName())).
                            collect(Collectors.toList());
                    for(Map.Entry<Integer, PutDataEvent> putDataEntry: filteredPutDataEvents){
                        if(putDataEntry.getKey() < getDataEntry.getKey()){
                            invalidGetDataEvents.remove(getDataEntry.getKey());
                        }
                    }
                }
                //lets check if the put data is after the get data
                for(Map.Entry<String, Integer> putDataEntry: invalidPutDataEvents.entrySet()){
                    if(getDataEvents.containsKey(putDataEntry.getKey()) && getDataEvents.get(putDataEntry.getKey()) < putDataEntry.getValue()){
                        invalidPutDataEvents.remove(putDataEntry.getKey());
                    }
                }
            }
            return invalidGetDataEvents.isEmpty() && invalidPutDataEvents.isEmpty();
        }

        private GetDataThatExistRule(){
        }
    }

    /**
     * Rule to ensure that start events occurred over stop servers and stops
     * events occurred over running servers.
     */
    private static class StartStopNodeRule extends UniverseRule {

        /**
         * Check if the composite event, desire state and real state, are all compliant
         * with the rule defined.
         *
         * @param compositeEvent    Composite event that is happening.
         * @param desireState       Current desire state of the universe.
         * @param realState         Current real state of the universe.
         * @return                  Whether the composite event, desire state and real state,
         *                          are all compliant with the rule.
         */
        @Override
        public boolean check(UniverseEventOperator compositeEvent, PhaseState desireState, PhaseState realState) {
            boolean pass = true;
            for(UniverseEvent event: compositeEvent.getSimpleEventsToCompose()){
                if(event instanceof ServerNodeEvent.Stop){
                    ServerNodeEvent.Stop stopEvent = (ServerNodeEvent.Stop)event;
                    ClusterStatusReport.NodeStatus status = desireState.getServers().get(stopEvent.getNodeName()).getStatus();
                    if(status == ClusterStatusReport.NodeStatus.DOWN){
                        pass = false;
                        break;
                    }
                }else if(event instanceof ServerNodeEvent.Start) {
                    ServerNodeEvent.Start startEvent = (ServerNodeEvent.Start)event;
                    ClusterStatusReport.NodeStatus status = desireState.getServers().get(startEvent.getNodeName()).getStatus();
                    if(status == ClusterStatusReport.NodeStatus.UP || status == ClusterStatusReport.NodeStatus.DB_SYNCING){
                        pass = false;
                        break;
                    }
                }
            }
            return pass;
        }

        private StartStopNodeRule(){
        }
    }
}
