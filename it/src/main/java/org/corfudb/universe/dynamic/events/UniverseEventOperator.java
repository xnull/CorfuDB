package org.corfudb.universe.dynamic.events;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.universe.dynamic.PhaseState;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Encapsulate the implementation of operation that allows to combine
 * multiple {@link UniverseEvent} to produce a Composite Event.
 * Three implementations are provided:
 *      - {@link Single}
 *      - {@link Concurrent}
 *      - {@link Sequential}
 *
 * Created by edilmo on 11/06/18.
 */
@Slf4j
public abstract class UniverseEventOperator implements UniverseEvent {

    /**
     * Get the list of simple events that this operator should compose.
     *
     * @return  Simple events to compose.
     */
    public abstract List<UniverseEvent> getSimpleEventsToCompose();

    /**
     * Get the desire-state of the universe after this composite event happened.
     * The application of the composite event is just the application of each of the
     * simple events that should be composed.
     * This method is called before {@link UniverseEventOperator::executeRealPartialTransition}.
     * The method must perform the updates directly over the currentDesireState reference.
     *
     * @param currentDesireState Desire-state of the universe before this composite event happened.
     * @return Desire-state of the universe after this event happened.
     */
    @Override
    public void applyDesirePartialTransition(PhaseState currentDesireState) {
        for(UniverseEvent simpleEvent: this.getSimpleEventsToCompose()){
            simpleEvent.applyDesirePartialTransition(currentDesireState);
        }
    }

    /**
     * Identity operator that allows to have a composite event of just one event.
     */
    public static class Single extends UniverseEventOperator {

        /**
         * The simple event to compose.
         */
        private final UniverseEvent simpleEvent;

        /**
         * Get the list of simple events that this operator should compose.
         *
         * @return Simple events to compose.
         */
        @Override
        public List<UniverseEvent> getSimpleEventsToCompose() {
            ArrayList<UniverseEvent> events = new ArrayList<>();
            events.add(this.simpleEvent);
            return events;
        }

        /**
         * Get a description of the observed change produced in the state of the universe
         * when this events happened.
         */
        @Override
        public String getObservationDescription() {
            StringBuilder builder = new StringBuilder();
            builder.append("Single event:\n");
            builder.append(this.simpleEvent.getObservationDescription());
            return builder.toString();
        }

        /**
         * Execute the transition of the universe that materialize the occurrence of the event.
         * The method must perform the updates directly over the @currentRealState reference.
         *
         * @param currentRealState Real-state of the universe before this event happened.
         */
        @Override
        public void executeRealPartialTransition(PhaseState currentRealState) {
            this.simpleEvent.executeRealPartialTransition(currentRealState);
        }

        public Single(UniverseEvent simpleEvent) {
            this.simpleEvent = simpleEvent;
        }
    }

    /**
     * Base operator that allows to execute multiple events.
     */
    private static abstract class Several extends UniverseEventOperator {

        /**
         * A short description of the observed change produced by this operator
         * in the state of the universe
         *
         * @return Short description of the operator.
         */
        protected abstract String getShortObservationDescription();

        /**
         * The list of events to compose.
         */
        protected final List<UniverseEvent> simpleEvents;

        /**
         * Get the list of simple events that this operator should compose.
         *
         * @return Simple events to compose.
         */
        @Override
        public List<UniverseEvent> getSimpleEventsToCompose() {
            return simpleEvents;
        }

        /**
         * Get a description of the observed change produced in the state of the universe
         * when this events happened.
         */
        @Override
        public String getObservationDescription() {
            StringBuilder builder = new StringBuilder();
            builder.append(this.getShortObservationDescription());
            builder.append(":");
            for(UniverseEvent simpleEvent: this.simpleEvents){
                builder.append("\n");
                builder.append(simpleEvent.getObservationDescription());
            }
            return builder.toString();
        }

        public Several(List<UniverseEvent> simpleEvents) {
            this.simpleEvents = simpleEvents;
        }
    }

    /**
     * Operator that allows to execute multiple events one immediately after the other.
     */
    public static class Sequential extends Several{

        /**
         * A short description of the observed change produced by this operator
         * in the state of the universe
         *
         * @return Short description of the operator.
         */
        @Override
        protected String getShortObservationDescription() {
            return "Sequential execution";
        }

        /**
         * Execute the transition of the universe that materialize the occurrence of the event.
         * The method must perform the updates directly over the {@link UniverseEventOperator::currentRealState} reference.
         *
         * @param currentRealState Real-state of the universe before this event happened.
         */
        @Override
        public void executeRealPartialTransition(PhaseState currentRealState) {
            for(UniverseEvent simpleEvent: this.simpleEvents){
                simpleEvent.executeRealPartialTransition(currentRealState);
            }
        }

        public Sequential(List<UniverseEvent> simpleEvents) {
            super(simpleEvents);
        }
    }

    /**
     * Operator that allows to execute multiple events concurrently.
     */
    public static class Concurrent extends Several{

        /**
         * A short description of the observed change produced by this operator
         * in the state of the universe
         *
         * @return Short description of the operator.
         */
        @Override
        protected String getShortObservationDescription() {
            return "Concurrent execution";
        }

        /**
         * Execute the transition of the universe that materialize the occurrence of the event.
         * The method must perform the updates directly over the {@link UniverseEventOperator::currentRealState} reference.
         *
         * @param currentRealState Real-state of the universe before this event happened.
         */
        @Override
        public void executeRealPartialTransition(PhaseState currentRealState) {
            try {
                CountDownLatch latch = new CountDownLatch(this.simpleEvents.size());
                ExecutorService executor = Executors.newFixedThreadPool(this.simpleEvents.size());
                for (UniverseEvent simpleEvent : this.simpleEvents) {
                    executor.submit(() -> {
                        simpleEvent.executeRealPartialTransition(currentRealState);
                        latch.countDown();
                    });
                }
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            catch (Exception ex) {
                log.error("Unexpected error executing events concurrently.", ex);
            }
        }

        public Concurrent(List<UniverseEvent> simpleEvents) {
            super(simpleEvents);
        }
    }
}