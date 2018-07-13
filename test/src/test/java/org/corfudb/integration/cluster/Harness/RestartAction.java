package org.corfudb.integration.cluster.Harness;

/**
 * An action that restarts a node
 * <p>Created by maithem on 7/18/18.
 */

public class RestartAction extends Action {

    public RestartAction(Node node) {
        this.node = node;
    }

    @Override
    public void run() {

    }
}
