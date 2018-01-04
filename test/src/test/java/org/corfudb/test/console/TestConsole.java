package org.corfudb.test.console;

import static org.fusesource.jansi.Ansi.ansi;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.test.CorfuTestExtension;
import org.corfudb.test.MemoryAppender;
import org.corfudb.test.parameters.Param;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.Ansi.Erase;

import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.extension.ExtensionContext;

public class TestConsole {

    public static TestConsole computer(@Nonnull String string) {
        return new TestConsole();
    }

    private static class TestConsoleEvent {
        @Getter
        final TestEvent event;

        @Getter
        final Object[] data;

        @Getter
        final CompletableFuture<Void> completion = new CompletableFuture<>();

        public TestConsoleEvent(@Nonnull TestEvent event, @Nullable Object[] data) {
            this.event = event;
            this.data = data;
        }

        public ExtensionContext getContext() {
            return (ExtensionContext) data[0];
        }
    }

    private enum TestEvent {
        TEST_START,
        TEST_END
        ;
        TestConsoleEvent getEvent(Object... data) {
            return new TestConsoleEvent(this, data);
        }
    }


    final BlockingDeque<TestConsoleEvent> eventQueue = new LinkedBlockingDeque<>();
    final Thread consoleThread;
    volatile boolean shutdown = false;
    final static Duration REFRESH_RATE = Duration.ofMillis(25);
    List<IConsoleAnimation> animations = new ArrayList<>();
    final Terminal terminal;

    private void doAnimations() {
        int total = 0;
        for (IConsoleAnimation animation : animations) {
            total += animation.animate();
            System.out.print(" ");
            total += 1;
        }
        if (total > 0) {
            System.out.print(ansi().cursorLeft(total));
        }
    }

    private void stopAnimations() {
        animations.clear();
        System.out.print(ansi().eraseLine(Erase.FORWARD));
    }

    private void addAnimation(@Nonnull IConsoleAnimation animation) {
        animations.add(animation);
    }

    public TestConsole() {
        try {
            terminal = TerminalBuilder.builder()
                .system(true)
                .dumb(true)
                .build();
        } catch (IOException e) {
            throw new UnrecoverableCorfuError(e);
        }

        consoleThread = new Thread(this::processEvents);
        consoleThread.setDaemon(true);
        consoleThread.setName("TestConsole");
        consoleThread.start();
    }

    private void cursorOff() {
        if (isInteractive()) {
            System.out.print("\u001b[?25l");
            System.out.flush();
        }
    }

    private void cursorOn() {
        if (isInteractive()) {
            System.out.print("\u001b[?25h");
            System.out.flush();
        }
    }

    int tabLevel = 0;

    enum LineLevel {
        SAME,
        CHILD,
        NEW_NODE,
        PREVIOUS
    }

    private int startLine(@Nonnull LineLevel level) {
        switch (level) {
            case SAME:
                printAtTab("┃  ");
                break;
            case NEW_NODE:
                printAtTab("┣─" );
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return tabLevel;
    }

    void printAtTab(String symbol) {
       for (int i = 0; i < tabLevel; i++) {
           System.out.print("┃ ");
       }
       System.out.print(symbol);
    }

    private void nextLine(@Nonnull LineLevel level) {
        nextLine(level, tabLevel);
    }
    private void nextLine(@Nonnull LineLevel level, int ofLevel) {
        if (level.equals(LineLevel.PREVIOUS)) {
            tabLevel = ofLevel - 1;
        } else if (level.equals(LineLevel.CHILD)) {
            tabLevel = ofLevel + 1;
        }
        System.out.println();
    }

    @Getter(lazy = true)
    private final boolean interactive = !terminal.getType().equals("dumb")
                                            && !Param.isTravisBuild();

    private void printThreads(int testLevel) {
        nextLine(LineLevel.CHILD, testLevel);
        startLine(LineLevel.NEW_NODE);

        System.out.print(Ansi.ansi().bold().a("Active Threads").reset());
        List<String> excludedThreads = ImmutableList.<String>builder()
            .add("process reaper")
            .add("Reference Handler")
            .add("Monitor Ctrl-Break")
            .add("Finalizer")
            .add("Signal Dispatcher")
            .add("surefire-forkedjvm-command-thread")
            .build();
        Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
        final LongAdder parkedThreads = new LongAdder();
        final List<Thread> sleepingThreads = new ArrayList<>();
        threads.forEach((thread, trace) -> {
            // Don't include the current thread or excluded threads
            if (!thread.equals(Thread.currentThread())
                && !excludedThreads.contains(thread.getName())) {
                if (trace.length > 0
                    && trace[0].getClassName().endsWith("Unsafe")
                    && trace[0].getMethodName().equals("park")) {
                    parkedThreads.increment();
                } else if (trace.length > 0
                    && trace[0].getClassName().equals("java.lang.ThreadParameter")
                    && trace[0].getMethodName().equals("sleep")) {
                    sleepingThreads.add(thread);
                } else {
                    nextLine(LineLevel.SAME);
                    startLine(LineLevel.SAME);
                    if (trace.length > 0) {
                        System.out.print(ansi().format("%-40s %s:%s",
                            thread.getName(),
                            trace[0].getClassName(),
                            trace[0].getMethodName()));
                    } else {
                        System.out.print(ansi().format("%-40s (no trace available)",
                            thread.getName()));
                    }
                }
            }
        });
        if (parkedThreads.longValue() > 0) {
            nextLine(LineLevel.SAME);
            startLine(LineLevel.SAME);
            System.out.print(ansi().fgBlue().format("Suppressed %d parked threads",
                parkedThreads.longValue()).reset());
        }
        if (sleepingThreads.size() > 0) {
            String sleepingThreadsString = sleepingThreads.stream()
                .map(Thread::getName)
                .collect(Collectors.joining(","));
            nextLine(LineLevel.SAME);
            startLine(LineLevel.SAME);
            System.out.print(ansi().fgMagenta()
                .format("Suppressed %d sleeping threads: ", sleepingThreads.size())
            .reset());
            final int maxLength = 100;
            Splitter.fixedLength(maxLength).split(sleepingThreadsString)
                .forEach(e -> {
                    nextLine(LineLevel.SAME);
                    startLine(LineLevel.SAME);
                    System.out.print(e);
                });
        }
    }

    private void printLog(int testLevel, ExtensionContext context) {
        nextLine(LineLevel.CHILD, testLevel);
        startLine(LineLevel.NEW_NODE);
        System.out.print(Ansi.ansi().bold().format("Last %d log messages",
            Param.LOG_BUFFER.getValue())
            .reset());
        MemoryAppender appender = context.getStore(CorfuTestExtension.NAMESPACE)
            .get(CorfuTestExtension.LOGGER, MemoryAppender.class);
        Iterable<byte[]> events = appender.getEventsAndReset();
        final int maxLength = 130;
        final int splitLength = 120;
        events.forEach(e -> {
            String eventString = new String(e);
            Arrays.stream(eventString.split("\n"))
                .forEach(eventLine -> {
                    String strippedLine = eventLine.replace("\n","")
                                                   .replace("\r", "");
                    if (strippedLine.length() < maxLength) {
                        if (strippedLine.trim().length() > 0) {
                            nextLine(LineLevel.SAME);
                            startLine(LineLevel.SAME);
                            System.out.print(strippedLine);
                        }
                    } else {
                        String firstPart = strippedLine.substring(0, maxLength);
                        nextLine(LineLevel.SAME);
                        startLine(LineLevel.SAME);
                        System.out.print(firstPart);

                        Splitter.fixedLength(splitLength).split(strippedLine.substring(maxLength))
                            .forEach(line -> {
                                if (line.trim().length() > 0) {
                                    nextLine(LineLevel.SAME);
                                    startLine(LineLevel.SAME);
                                    System.out.print("                   "  + line);
                                }
                            });
                    }
                });
        });
        nextLine(LineLevel.PREVIOUS);
    }

    private void processEvents() {
        cursorOff();
        try {
            while (!shutdown) {
                // Check if there is an event in the queue, timing out in REFRESH_RATE duration.
                TestConsoleEvent event = eventQueue
                    .pollFirst(REFRESH_RATE.toMillis(), TimeUnit.MILLISECONDS);

                // If there is, process it
                if (event != null) {
                    switch (event.getEvent()) {
                        case TEST_START:
                            startLine(LineLevel.NEW_NODE);
                            System.out.print(ansi().a(event.getContext().getDisplayName()));
                            System.out.print(" ");
                            System.out.flush();
                            addAnimation(new Spinner());
                            addAnimation(new ConsoleCounter(System.currentTimeMillis()));
                            break;
                        case TEST_END:
                            stopAnimations();
                            int testLevel = tabLevel;
                            if (event.getContext().getExecutionException().isPresent()) {
                                System.out.print(Ansi.ansi().fgRed().a(" ✘").reset());
                            } else {
                                System.out.print(Ansi.ansi().fgGreen().a(" ✔").reset());
                            }
                            if (event.getContext().getExecutionException().isPresent()) {
                                Throwable throwable =
                                    event.getContext().getExecutionException().get();
                                nextLine(LineLevel.CHILD, testLevel);
                                int exceptionLevel = startLine(LineLevel.NEW_NODE);
                                System.out.print(Ansi.ansi().bold().a("Exception: ").reset());
                                System.out.print(Ansi.ansi()
                                     .fgRed().a(throwable.getClass().getSimpleName()).reset());
                                if (throwable.getMessage() != null
                                    && !throwable.getMessage().equals("")) {
                                    nextLine(LineLevel.SAME, exceptionLevel);
                                    startLine(LineLevel.SAME);
                                    System.out.print("Message: " + throwable.getMessage());
                                }
                                if (throwable.getStackTrace().length != 0) {
                                    Optional<StackTraceElement> element =
                                        Arrays.stream(throwable.getStackTrace())
                                        .filter(x -> !x.isNativeMethod())
                                        .filter(x -> !x.getClassName().startsWith("sun.reflect"))
                                        .filter(x ->
                                            !x.getClassName().startsWith("java.lang.reflect"))
                                        .findFirst();

                                    StackTraceElement exceptionElement =
                                        element.orElse(throwable.getStackTrace()[0]);
                                    nextLine(LineLevel.SAME, exceptionLevel);
                                    startLine(LineLevel.SAME);
                                    System.out.print("Method: "
                                        + Ansi.ansi()
                                        .format("%s::%s",
                                            exceptionElement.getClassName()
                                                .substring(exceptionElement.getClassName()
                                                    .lastIndexOf('.') + 1),
                                            exceptionElement.getMethodName()));
                                    nextLine(LineLevel.SAME, exceptionLevel);
                                    startLine(LineLevel.SAME);
                                    System.out.print("File: "
                                        + Ansi.ansi()
                                        .format("%s:%d",
                                            exceptionElement.getFileName(),
                                            exceptionElement.getLineNumber()));
                                }
                                printThreads(testLevel);
                                printLog(testLevel, event.getContext());
                                nextLine(LineLevel.PREVIOUS);
                            } else {
                                nextLine(LineLevel.SAME);
                            }
                            break;
                    }

                    event.completion.complete(null);
                }

                // Console updates
                if (isInteractive()) {
                    doAnimations();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            cursorOn();
        }
    }

    public void shutdown() {
        shutdown = true;
        consoleThread.interrupt();
    }

    public void startTestExecution(@Nonnull ExtensionContext context) {
        eventQueue.add(TestEvent.TEST_START.getEvent(context));
    }

    public void endTestExecution(@Nonnull ExtensionContext context) {
        TestConsoleEvent event = TestEvent.TEST_END.getEvent(context);
        eventQueue.add(event);

        // Block until the thread is done...
        event.completion.join();
    }
}
