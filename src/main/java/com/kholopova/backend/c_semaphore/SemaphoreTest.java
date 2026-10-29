package com.kholopova.backend.c_semaphore;

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
public class SemaphoreTest {
  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("bench")) {
      benchmarkAcquireRelease(1_000_000);
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

  private record Case(String name, long timeoutMs, Runnable body) {
  }

  private record Result(String name, boolean passed, String message) {
  }

  private static List<Case> cases() {
    List<Case> list = new ArrayList<>();
    list.add(new Case("TestAcquireRelease", 3_000, SemaphoreTest::testAcquireRelease));
    list.add(new Case("TestBlocksWhenEmpty", 8_000, SemaphoreTest::testBlocksWhenEmpty));
    list.add(new Case("TestNeverMoreThanLimit", 15_000, SemaphoreTest::testNeverMoreThanLimit));
    list.add(new Case("TestTryAcquire", 3_000, SemaphoreTest::testTryAcquire));
    list.add(new Case("TestZeroPermits", 8_000, SemaphoreTest::testZeroPermits));
    list.add(new Case("TestAllWaitersWakeUp", 15_000, SemaphoreTest::testAllWaitersWakeUp));
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

  /**
   * Горутина, которая выполняет действие и позволяет дождаться её с таймаутом.
   */
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

  private static void testAcquireRelease() {
    Semaphore semaphore = new Semaphore(2);
    expectAvailable(semaphore, 2);

    semaphore.acquire();
    semaphore.acquire();
    expectAvailable(semaphore, 0);

    semaphore.release();
    semaphore.release();
    expectAvailable(semaphore, 2);
  }

  private static void expectAvailable(Semaphore semaphore, int want) {
    int got = semaphore.available();
    if (got != want) {
      throw new AssertionError("Available вернул " + got + ", ожидалось " + want);
    }
  }

  private static void testBlocksWhenEmpty() {
    Semaphore semaphore = new Semaphore(1);
    semaphore.acquire();

    Task<Void> second = Task.start(() -> {
      semaphore.acquire();
      semaphore.release();
      return null;
    });

    try {
      if (second.finished(50)) {
        throw new AssertionError("Acquire прошёл, хотя разрешений нет");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    semaphore.release();

    try {
      if (!second.finished(2000)) {
        throw new AssertionError("Acquire не проснулся после Release");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testNeverMoreThanLimit() {
    final int limit = 3;
    Semaphore semaphore = new Semaphore(limit);
    AtomicInteger inside = new AtomicInteger();
    AtomicInteger peak = new AtomicInteger();
    Throwable[] error = {null};

    Thread[] workers = startWorkers(40, () -> {
      semaphore.acquire();
      int now = inside.incrementAndGet();
      while (true) {
        int old = peak.get();
        if (now <= old || peak.compareAndSet(old, now)) {
          break;
        }
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      inside.decrementAndGet();
      semaphore.release();
    }, error);

    Task<Void> all = Task.start(() -> {
      try {
        awaitWorkers(workers, error);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return null;
    });
    try {
      if (!all.finished(10_000)) {
        throw new AssertionError("горутины не дождались разрешений");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    int highest = peak.get();
    if (highest > limit) {
      throw new AssertionError("одновременно внутри было " + highest + ", лимит " + limit);
    }
    if (highest < 2) {
      throw new AssertionError("параллелизма не случилось вовсе: пик " + highest);
    }
  }

  private static void testTryAcquire() {
    Semaphore semaphore = new Semaphore(1);
    if (!semaphore.tryAcquire()) {
      throw new AssertionError("TryAcquire при свободном разрешении должен получиться");
    }
    if (semaphore.tryAcquire()) {
      throw new AssertionError("TryAcquire без разрешений должен вернуть false");
    }

    semaphore.release();

    if (!semaphore.tryAcquire()) {
      throw new AssertionError("после Release разрешение снова доступно");
    }
  }

  private static void testZeroPermits() {
    Semaphore semaphore = new Semaphore(0);
    if (semaphore.tryAcquire()) {
      throw new AssertionError("у семафора на ноль разрешений брать нечего");
    }

    Task<Void> waiting = Task.start(() -> {
      semaphore.acquire();
      return null;
    });
    try {
      if (waiting.finished(50)) {
        throw new AssertionError("Acquire прошёл на пустом семафоре");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    semaphore.release();

    try {
      if (!waiting.finished(2000)) {
        throw new AssertionError("Acquire не проснулся");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testAllWaitersWakeUp() {
    Semaphore semaphore = new Semaphore(0);
    final int waiters = 30;
    Throwable[] error = {null};
    Thread[] workers = startWorkers(waiters, semaphore::acquire, error);

    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    for (int i = 0; i < waiters; i++) {
      semaphore.release();
    }

    Task<Void> all = Task.start(() -> {
      try {
        awaitWorkers(workers, error);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return null;
    });
    try {
      if (!all.finished(10_000)) {
        throw new AssertionError("не все ждущие проснулись");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  /**
   * Аналог BenchmarkAcquireRelease.
   */
  private static void benchmarkAcquireRelease(int n) {
    Semaphore semaphore = new Semaphore(1);
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      semaphore.acquire();
      semaphore.release();
    }
    long elapsed = System.nanoTime() - start;
    System.out.printf("BenchmarkAcquireRelease %d ops, %.1f ns/op%n", n, (double) elapsed / n);
  }
}

