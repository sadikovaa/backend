package com.kholopova.backend.e_once;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Тесты переписала на Java при помощи ИИ
 */
public class OnceTest {
    public static void main(String[] args) {
        int failed = runAll();
        System.exit(failed == 0 ? 0 : 1);
    }

    private static int runAll() {
        int failed = 0;
        for (Case test : cases()) {
            Result result = runCase(test);
            if (result.passed) {
                System.out.println("--- PASS: " + result.name);
            } else {
                failed++;
                System.out.println("--- FAIL: " + result.name);
                System.out.println("    " + result.message);
            }
        }
        System.out.println(failed == 0 ? "OK" : "FAILED: " + failed);
        return failed;
    }

    private record Case(String name, long timeoutMs, Runnable body) {}

    private record Result(String name, boolean passed, String message) {}

    private static List<Case> cases() {
        List<Case> list = new ArrayList<>();
        list.add(new Case("TestRunsOnlyOnce", 3_000, OnceTest::testRunsOnlyOnce));
        list.add(new Case("TestDone", 3_000, OnceTest::testDone));
        list.add(new Case("TestConcurrentCallsRunItOnce", 15_000, OnceTest::testConcurrentCallsRunItOnce));
        list.add(new Case("TestDoWaitsForTheWinner", 15_000, OnceTest::testDoWaitsForTheWinner));
        list.add(new Case("TestPanicCountsAsDone", 3_000, OnceTest::testPanicCountsAsDone));
        list.add(new Case("TestStress", 60_000, OnceTest::testStress));
        return list;
    }

    private static Result runCase(Case test) {
        ExecutorService pool = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, test.name);
            thread.setDaemon(true);
            return thread;
        });
        Future<Throwable> future = pool.submit(() -> {
            try {
                test.body.run();
                return null;
            } catch (Throwable error) {
                return error;
            }
        });
        try {
            Throwable error = future.get(test.timeoutMs, TimeUnit.MILLISECONDS);
            if (error == null) {
                return new Result(test.name, true, "");
            }
            return new Result(test.name, false, message(error));
        } catch (TimeoutException e) {
            future.cancel(true);
            return new Result(test.name, false, "превышено время ожидания");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Result(test.name, false, "прогон прерван");
        } catch (ExecutionException e) {
            return new Result(test.name, false, message(e.getCause()));
        } finally {
            pool.shutdownNow();
        }
    }

    private static String message(Throwable error) {
        if (error == null) {
            return "";
        }
        String text = error.getMessage();
        if (text == null || text.isEmpty()) {
            return error.getClass().getSimpleName();
        }
        return text;
    }

    private static RuntimeException asRuntime(Throwable error) {
        if (error instanceof RuntimeException runtime) {
            return runtime;
        }
        if (error instanceof Error err) {
            throw err;
        }
        return new RuntimeException(error);
    }

    /** Горутина, которая выполняет действие и позволяет дождаться её с таймаутом. */
    private static final class Task<T> {
        private final Thread thread;
        private volatile boolean done;
        private volatile T value;
        private volatile Throwable error;

        static <T> Task<T> start(Supplier<T> action) {
            Task<T> task = new Task<>(action);
            task.thread.setDaemon(true);
            task.thread.start();
            return task;
        }

        private Task(Supplier<T> action) {
            thread = new Thread(() -> {
                try {
                    value = action.get();
                } catch (Throwable e) {
                    error = e;
                } finally {
                    done = true;
                }
            });
        }

        boolean finished(long timeoutMs) throws InterruptedException {
            thread.join(timeoutMs);
            if (!done) {
                return false;
            }
            if (error != null) {
                throw asRuntime(error);
            }
            return true;
        }
    }

    private static Thread[] startWorkers(int n, Runnable body, Throwable[] error) {
        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            threads[i] = new Thread(() -> {
                try {
                    body.run();
                } catch (Throwable thrown) {
                    synchronized (error) {
                        if (error[0] == null) {
                            error[0] = thrown;
                        }
                    }
                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }
        return threads;
    }

    private static void awaitWorkers(Thread[] threads, Throwable[] error) throws InterruptedException {
        for (Thread thread : threads) {
            thread.join();
        }
        if (error[0] != null) {
            throw asRuntime(error[0]);
        }
    }

    private static void testRunsOnlyOnce() {
        Once once = new Once();
        int[] calls = {0};
        for (int i = 0; i < 10; i++) {
            once.run(() -> calls[0]++);
        }
        if (calls[0] != 1) {
            throw new AssertionError("функция вызвана " + calls[0] + " раз");
        }
    }

    private static void testDone() {
        Once once = new Once();
        if (once.done()) {
            throw new AssertionError("до первого Do работа не сделана");
        }
        once.run(() -> {
        });
        if (!once.done()) {
            throw new AssertionError("после Do работа сделана");
        }
    }

    private static void testConcurrentCallsRunItOnce() {
        Once once = new Once();
        AtomicInteger calls = new AtomicInteger();
        Throwable[] error = {null};
        Thread[] threads = startWorkers(100, () -> once.run(() -> {
            calls.incrementAndGet();
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }), error);

        Task<Void> all = Task.start(() -> {
            try {
                awaitWorkers(threads, error);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return null;
        });
        try {
            if (!all.finished(10_000)) {
                throw new AssertionError("часть горутин зависла в Do");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        int got = calls.get();
        if (got != 1) {
            throw new AssertionError("функция вызвана " + got + " раз");
        }
    }

    private static void testDoWaitsForTheWinner() {
        Once once = new Once();
        AtomicInteger ready = new AtomicInteger();

        Task<Void> first = Task.start(() -> {
            once.run(() -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                ready.set(1);
            });
            return null;
        });

        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        Task<Void> second = Task.start(() -> {
            once.run(() -> {
            });
            if (ready.get() != 1) {
                throw new AssertionError("Do вернулся до того, как работа была закончена");
            }
            return null;
        });

        try {
            if (!first.finished(5000)) {
                throw new AssertionError("первый Do завис");
            }
            if (!second.finished(5000)) {
                throw new AssertionError("второй Do завис");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void testPanicCountsAsDone() {
        Once once = new Once();
        int[] calls = {0};
        try {
            once.run(() -> {
                calls[0]++;
                throw new RuntimeException("упало");
            });
        } catch (RuntimeException ignored) {
            // recover() в Go глотает панику, в том числе «не реализовано».
        }
        once.run(() -> calls[0]++);
        if (calls[0] != 1) {
            throw new AssertionError("после паники функция вызвана ещё раз: всего " + calls[0]);
        }
    }

    private static void testStress() {
        for (int round = 0; round < 300; round++) {
            Once once = new Once();
            AtomicInteger calls = new AtomicInteger();
            Throwable[] error = {null};
            Thread[] threads = startWorkers(16, () -> once.run(() -> calls.incrementAndGet()), error);
            try {
                awaitWorkers(threads, error);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            int got = calls.get();
            if (got != 1) {
                throw new AssertionError("раунд " + round + ": вызовов " + got);
            }
        }
    }
}
