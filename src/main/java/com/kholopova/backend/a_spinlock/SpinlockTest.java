package com.kholopova.backend.a_spinlock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Тесты переписала на Java при помощи ИИ
 */
public class SpinlockTest {
  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("bench")) {
      benchmarkUncontended(1_000_000);
      benchmarkContended(200_000);
      return;
    }
    int failed = runConsole();
    System.exit(failed == 0 ? 0 : 1);
  }

  private static int runConsole() {
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

  private record Result(String name, boolean passed, long millis, String message) {
  }

  private static List<Case> cases() {
    List<Case> list = new ArrayList<>();
    add(list, "TestLockUnlock", 5_000, SpinlockTest::testLockUnlock);
    add(list, "TestMutualExclusion", 30_000, SpinlockTest::testMutualExclusion);
    add(list, "TestOnlyOneInside", 15_000, SpinlockTest::testOnlyOneInside);
    add(list, "TestTryLock", 5_000, SpinlockTest::testTryLock);
    add(list, "TestLockWaitsForUnlock", 8_000, SpinlockTest::testLockWaitsForUnlock);
    add(list, "TestUnlockWithoutLockPanics", 3_000, SpinlockTest::testUnlockWithoutLockPanics);
    return list;
  }

  private static void add(List<Case> list, String name, long timeoutMs, Consumer<Locker> body) {
    list.add(new Case(name + "/Spinlock", timeoutMs, () -> body.accept(new Spinlock())));
    list.add(new Case(name + "/TTAS", timeoutMs, () -> body.accept(new TTAS())));
  }

  private static Result runCase(Case test) {
    long start = System.nanoTime();
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
      long millis = millisSince(start);
      if (error == null) {
        return new Result(test.name, true, millis, "");
      }
      return new Result(test.name, false, millis, message(error));
    } catch (TimeoutException e) {
      future.cancel(true);
      return new Result(test.name, false, millisSince(start), "превышено время ожидания");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return new Result(test.name, false, millisSince(start), "прогон прерван");
    } catch (ExecutionException e) {
      return new Result(test.name, false, millisSince(start), message(e.getCause()));
    } finally {
      pool.shutdownNow();
    }
  }

  private static long millisSince(long startNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
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

    /**
     * Ждёт завершения не дольше {@code timeoutMs}.
     *
     * @return {@code false}, если поток ещё работает
     */
    boolean finished(long timeoutMs) throws InterruptedException {
      thread.join(timeoutMs);
      if (!done) {
        return false;
      }
      if (error != null) {
        if (error instanceof RuntimeException runtime) {
          throw runtime;
        }
        if (error instanceof Error err) {
          throw err;
        }
        throw new RuntimeException(error);
      }
      return true;
    }

    T result() {
      return value;
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
      if (error[0] instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (error[0] instanceof Error err) {
        throw err;
      }
      throw new RuntimeException(error[0]);
    }
  }

  private static void testLockUnlock(Locker lock) {
    lock.lock();
    lock.unlock();
    lock.lock();
    lock.unlock();
  }

  private static void testMutualExclusion(Locker lock) {
    final int threads = 8;
    final int iterations = 20_000;
    int[] counter = {0};
    Throwable[] error = {null};

    Thread[] workers = startWorkers(threads, () -> {
      for (int j = 0; j < iterations; j++) {
        lock.lock();
        counter[0]++;
        lock.unlock();
      }
    }, error);

    try {
      awaitWorkers(workers, error);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    int want = threads * iterations;
    if (counter[0] != want) {
      throw new AssertionError("счётчик " + counter[0] + ", ожидалось " + want);
    }
  }

  private static void testOnlyOneInside(Locker lock) {
    int[] inside = {0};
    boolean[] bad = {false};
    Throwable[] error = {null};

    Thread[] workers = startWorkers(8, () -> {
      for (int j = 0; j < 2000; j++) {
        lock.lock();
        inside[0]++;
        if (inside[0] != 1) {
          bad[0] = true;
        }
        inside[0]--;
        lock.unlock();
      }
    }, error);

    try {
      awaitWorkers(workers, error);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    if (bad[0]) {
      throw new AssertionError("в критической секции оказалось больше одной горутины");
    }
  }

  private static void testTryLock(Locker lock) {
    if (!lock.tryLock()) {
      throw new AssertionError("TryLock на свободном замке должен получиться");
    }

    Task<Boolean> got = Task.start(lock::tryLock);
    try {
      if (!got.finished(1000)) {
        throw new AssertionError("TryLock заблокировался, а не должен");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    if (got.result()) {
      throw new AssertionError("TryLock на занятом замке должен вернуть false");
    }

    lock.unlock();
    if (!lock.tryLock()) {
      throw new AssertionError("после Unlock замок снова свободен");
    }
    lock.unlock();
  }

  private static void testLockWaitsForUnlock(Locker lock) {
    lock.lock();

    Task<Void> second = Task.start(() -> {
      lock.lock();
      lock.unlock();
      return null;
    });

    try {
      if (second.finished(50)) {
        throw new AssertionError("второй Lock прошёл, пока замок занят");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    lock.unlock();

    try {
      if (!second.finished(2000)) {
        throw new AssertionError("второй Lock не проснулся после Unlock");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testUnlockWithoutLockPanics(Locker lock) {
    try {
      lock.unlock();
    } catch (RuntimeException expected) {
      return;
    }
    throw new AssertionError("Unlock без Lock должен паниковать");
  }

  /**
   * Аналог BenchmarkUncontended.
   */
  private static void benchmarkUncontended(int n) {
    Spinlock lock = new Spinlock();
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      lock.lock();
      lock.unlock();
    }
    long elapsed = System.nanoTime() - start;
    System.out.printf("BenchmarkUncontended %d ops, %.1f ns/op%n", n, (double) elapsed / n);
  }

  /**
   * Аналог BenchmarkContended: итерации делятся между потоками по числу ядер.
   */
  private static void benchmarkContended(int n) {
    Spinlock lock = new Spinlock();
    int procs = Runtime.getRuntime().availableProcessors();
    int perThread = Math.max(1, n / procs);
    Thread[] threads = new Thread[procs];
    for (int i = 0; i < procs; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < perThread; j++) {
          lock.lock();
          lock.unlock();
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
    System.out.printf("BenchmarkContended %d ops, %.1f ns/op%n", ops, (double) elapsed / ops);
  }
}
