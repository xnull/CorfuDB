package org.corfudb.integration.cluster.Harness;

/**
 * An action that pauses a node
 * <p>Created by maithem on 7/18/18.
 */

public class PauseAction extends Action {

    public PauseAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() {

    }
}
