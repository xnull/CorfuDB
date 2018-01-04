package org.corfudb.test.console;

import java.util.concurrent.TimeUnit;

public class ConsoleCounter implements IConsoleAnimation {
    final long startTime;
    static final int COUNTER_SIZE = 7;

    public ConsoleCounter(long startTime) {
        this.startTime = startTime;
    }


    @Override
    public int animate() {
        long currentTime = System.currentTimeMillis();
        long difference = currentTime - startTime;
        System.out.printf("[%02d:%02d]", TimeUnit.MILLISECONDS.toMinutes(difference),
            TimeUnit.MILLISECONDS.toSeconds(difference));
        return COUNTER_SIZE;
    }
}
