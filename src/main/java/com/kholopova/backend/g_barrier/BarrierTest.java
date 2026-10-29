package com.kholopova.backend.g_barrier;

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
public class BarrierTest {
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
        list.add(new Case("TestNobodyPassesEarly", 8_000, BarrierTest::testNobodyPassesEarly));
        list.add(new Case("TestSingleParticipant", 3_000, BarrierTest::testSingleParticipant));
        list.add(new Case("TestReusableAcrossRounds", 45_000, BarrierTest::testReusableAcrossRounds));
        list.add(new Case("TestRoundsDoNotOverlap", 45_000, BarrierTest::testRoundsDoNotOverlap));
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
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

    private static void testNobodyPassesEarly() {
        Barrier barrier = new Barrier(3);
        AtomicInteger passed = new AtomicInteger();
        Throwable[] error = {null};

        startWorkers(2, () -> {
            barrier.await();
            passed.incrementAndGet();
        }, error);

        sleep(100);
        int early = passed.get();
        if (early != 0) {
            throw new AssertionError(early + " участников прошли барьер до прихода всех");
        }
        if (error[0] != null) {
            throw asRuntime(error[0]);
        }

        Task<Void> last = Task.start(() -> {
            barrier.await();
            passed.incrementAndGet();
            return null;
        });
        try {
            if (!last.finished(2000)) {
                throw new AssertionError("последний участник не прошёл барьер");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (passed.get() != 3 && System.nanoTime() < deadline) {
            if (error[0] != null) {
                throw asRuntime(error[0]);
            }
            sleep(1);
        }
        int got = passed.get();
        if (got != 3) {
            throw new AssertionError("барьер прошли " + got + " из 3");
        }
    }

    private static void testSingleParticipant() {
        Barrier barrier = new Barrier(1);
        Task<Void> waiting = Task.start(() -> {
            barrier.await();
            return null;
        });
        try {
            if (!waiting.finished(1000)) {
                throw new AssertionError("барьер на одного не должен блокировать");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void testReusableAcrossRounds() {
        final int parties = 6;
        final int rounds = 50;
        Barrier barrier = new Barrier(parties);
        AtomicInteger passes = new AtomicInteger();
        Throwable[] error = {null};

        Thread[] threads = startWorkers(parties, () -> {
            for (int r = 0; r < rounds; r++) {
                barrier.await();
                passes.incrementAndGet();
            }
        }, error);

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
            if (!all.finished(30_000)) {
                throw new AssertionError("барьер завис между раундами");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        int got = passes.get();
        int want = parties * rounds;
        if (got != want) {
            throw new AssertionError("проходов " + got + ", ожидалось " + want);
        }
    }

    private static void testRoundsDoNotOverlap() {
        final int parties = 4;
        final int rounds = 100;
        Barrier barrier = new Barrier(parties);
        AtomicInteger inRound = new AtomicInteger();
        AtomicInteger bad = new AtomicInteger();
        Throwable[] error = {null};

        Thread[] threads = startWorkers(parties, () -> {
            for (int r = 0; r < rounds; r++) {
                barrier.await();
                if (inRound.incrementAndGet() > parties) {
                    bad.set(1);
                }
                try {
                    Thread.sleep(0, 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                inRound.decrementAndGet();
            }
        }, error);

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
            if (!all.finished(30_000)) {
                throw new AssertionError("тест завис");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        if (bad.get() != 0) {
            throw new AssertionError("участники следующего раунда обогнали предыдущий");
        }
    }
}
