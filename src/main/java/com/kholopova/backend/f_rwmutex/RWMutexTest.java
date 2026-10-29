package com.kholopova.backend.f_rwmutex;

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
public class RWMutexTest {
    public static void main(String[] args) {
        if (args.length > 0 && args[0].equals("bench")) {
            benchmarkReadHeavy(200_000);
            return;
        }
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
        list.add(new Case("TestWriterExcludesEveryone", 8_000, RWMutexTest::testWriterExcludesEveryone));
        list.add(new Case("TestReadersGoTogether", 8_000, RWMutexTest::testReadersGoTogether));
        list.add(new Case("TestWriterWaitsForReaders", 8_000, RWMutexTest::testWriterWaitsForReaders));
        list.add(new Case("TestNoTornState", 45_000, RWMutexTest::testNoTornState));
        list.add(new Case("TestUnlockWithoutLockPanics/Unlock", 3_000, RWMutexTest::testUnlockWithoutLockPanics));
        list.add(new Case("TestUnlockWithoutLockPanics/RUnlock", 3_000, RWMutexTest::testRUnlockWithoutRLockPanics));
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

        /** Как ветка {@code select}: поток уже закончился — это провал ожидания. */
        void failIfDone(String message) {
            if (!done) {
                return;
            }
            if (error != null) {
                throw asRuntime(error);
            }
            throw new AssertionError(message);
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

    private static void testWriterExcludesEveryone() {
        RWMutex rw = new RWMutex();
        rw.lock();

        Task<Void> reader = Task.start(() -> {
            rw.rLock();
            rw.rUnlock();
            return null;
        });
        Task<Void> writer = Task.start(() -> {
            rw.lock();
            rw.unlock();
            return null;
        });

        sleep(50);
        reader.failIfDone("читатель прошёл, пока держит писатель");
        writer.failIfDone("второй писатель прошёл, пока держит первый");

        rw.unlock();

        try {
            if (!reader.finished(2000)) {
                throw new AssertionError("читатель не проснулся");
            }
            if (!writer.finished(2000)) {
                throw new AssertionError("писатель не проснулся");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void testReadersGoTogether() {
        RWMutex rw = new RWMutex();
        final int readers = 8;
        AtomicInteger inside = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Throwable[] error = {null};

        Thread[] threads = startWorkers(readers, () -> {
            rw.rLock();
            int now = inside.incrementAndGet();
            while (true) {
                int old = peak.get();
                if (now <= old || peak.compareAndSet(old, now)) {
                    break;
                }
            }
            sleep(30);
            inside.decrementAndGet();
            rw.rUnlock();
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
            if (!all.finished(5000)) {
                throw new AssertionError("читатели зависли");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        int highest = peak.get();
        if (highest < 2) {
            throw new AssertionError("читатели шли по одному (пик " + highest + ") — смысл RWMutex теряется");
        }
    }

    private static void testWriterWaitsForReaders() {
        RWMutex rw = new RWMutex();
        rw.rLock();
        rw.rLock();

        Task<Void> writer = Task.start(() -> {
            rw.lock();
            rw.unlock();
            return null;
        });

        sleep(50);
        writer.failIfDone("писатель прошёл при живых читателях");

        rw.rUnlock();

        sleep(50);
        writer.failIfDone("писатель прошёл, пока остался один читатель");

        rw.rUnlock();

        try {
            if (!writer.finished(2000)) {
                throw new AssertionError("писатель не проснулся после последнего RUnlock");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static void testNoTornState() {
        RWMutex rw = new RWMutex();
        int[] shared = {0};
        AtomicInteger bad = new AtomicInteger();
        Throwable[] error = {null};

        Thread[] writers = startWorkers(4, () -> {
            for (int j = 0; j < 5000; j++) {
                rw.lock();
                shared[0]++;
                shared[0]++;
                rw.unlock();
            }
        }, error);
        Thread[] readers = startWorkers(8, () -> {
            for (int j = 0; j < 5000; j++) {
                rw.rLock();
                if (shared[0] % 2 != 0) {
                    bad.set(1);
                }
                rw.rUnlock();
            }
        }, error);

        Thread[] all = new Thread[writers.length + readers.length];
        System.arraycopy(writers, 0, all, 0, writers.length);
        System.arraycopy(readers, 0, all, writers.length, readers.length);

        Task<Void> done = Task.start(() -> {
            try {
                awaitWorkers(all, error);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return null;
        });
        try {
            if (!done.finished(30_000)) {
                throw new AssertionError("нагрузочный тест завис");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }

        if (bad.get() != 0) {
            throw new AssertionError("читатель увидел состояние в середине записи");
        }
        int want = 4 * 5000 * 2;
        if (shared[0] != want) {
            throw new AssertionError("записи потерялись: " + shared[0]);
        }
    }

    private static void testUnlockWithoutLockPanics() {
        RWMutex rw = new RWMutex();
        try {
            rw.unlock();
        } catch (RuntimeException expected) {
            return;
        }
        throw new AssertionError("Unlock без Lock должен паниковать");
    }

    private static void testRUnlockWithoutRLockPanics() {
        RWMutex rw = new RWMutex();
        try {
            rw.rUnlock();
        } catch (RuntimeException expected) {
            return;
        }
        throw new AssertionError("RUnlock без RLock должен паниковать");
    }

    /** Аналог BenchmarkReadHeavy. */
    private static void benchmarkReadHeavy(int n) {
        RWMutex rw = new RWMutex();
        int procs = Runtime.getRuntime().availableProcessors();
        int perThread = Math.max(1, n / procs);
        Thread[] threads = new Thread[procs];
        for (int i = 0; i < procs; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < perThread; j++) {
                    rw.rLock();
                    rw.rUnlock();
                }
            });
        }
        long start = System.nanoTime();
        for (Thread thread : threads) {
            thread.start();
        }
        try {
            for (Thread thread : threads) {
                thread.join();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        int ops = perThread * procs;
        long elapsed = System.nanoTime() - start;
        System.out.printf("BenchmarkReadHeavy %d ops, %.1f ns/op%n", ops, (double) elapsed / ops);
    }
}
