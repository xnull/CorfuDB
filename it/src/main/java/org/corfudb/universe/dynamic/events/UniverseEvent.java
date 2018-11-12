package org.corfudb.universe.dynamic.events;

import org.corfudb.universe.dynamic.Dynamic;
import org.corfudb.universe.dynamic.PhaseState;

/**
 * Defines the contract to follow by any event that can happened in the universe.
 *
 * The universe event is the abstraction that allows to encapsulate different specific
 * tasks that have some effect in the state of the universe. e.g:
 *      Management Tasks:
 *          - Remove Node
 *          - Add Node
 *          - Force Remove Node (hand of god)
 *      Data tasks:
 *          - Put Data
 *          - Get Data
 *      IT Tasks:
 *          - Kill Node
 *          - Stop Node
 *          - Start Node
 *          - Disconnect Node
 *          - Reconnect Node
 *          - Pause Node
 *          - Resume Node
 *          - Wait for unresponsive nodes
 *
 * Created by edilmo on 11/06/18.
 */
public interface UniverseEvent {

    /**
     * Get a description of the observed change produced in the state of the universe
     * when this events happened.
     */
    String getObservationDescription();

    /**
     * Get the desire-state of the universe after this event happened.
     * This method is called before {@link UniverseEvent::executeRealPartialTransition}.
     * The method must perform the updates directly over the {@link UniverseEvent::currentDesireState} reference.
     *
     * @param currentDesireState    Desire-state of the universe before this event happened.
     * @return                      Desire-state of the universe after this event happened.
     */
    void applyDesirePartialTransition(PhaseState currentDesireState);

    /**
     * Execute the transition of the universe that materialize the occurrence of the event.
     * The method must perform the updates directly over the parameter currentRealState reference.
     *
     * @param currentRealState  Real-state of the universe before this event happened.
     */
    void executeRealPartialTransition(PhaseState currentRealState);
}
