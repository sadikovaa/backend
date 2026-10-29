package com.kholopova.backend.b_mutex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Тесты переписала на Java при помощи ИИ
 */
public class MutexTest {
  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals("bench")) {
      benchmarkUncontended(1_000_000);
      benchmarkContended(200_000);
      benchmarkStdlibContended(200_000);
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
    list.add(new Case("TestLockUnlock", 5_000, MutexTest::testLockUnlock));
    list.add(new Case("TestMutualExclusion", 30_000, MutexTest::testMutualExclusion));
    list.add(new Case("TestOnlyOneInside", 20_000, MutexTest::testOnlyOneInside));
    list.add(new Case("TestLockWaitsForUnlock", 8_000, MutexTest::testLockWaitsForUnlock));
    list.add(new Case("TestEveryWaiterWakesUp", 15_000, MutexTest::testEveryWaiterWakesUp));
    list.add(new Case("TestTryLock", 5_000, MutexTest::testTryLock));
    list.add(new Case("TestUnlockWithoutLockPanics", 3_000, MutexTest::testUnlockWithoutLockPanics));
    list.add(new Case("TestHandoffUnderLoad", 10_000, MutexTest::testHandoffUnderLoad));
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
      throw asRuntime(error[0]);
    }
  }

  private static void testLockUnlock() {
    Mutex mutex = new Mutex();
    mutex.lock();
    mutex.unlock();
    mutex.lock();
    mutex.unlock();
  }

  private static void testMutualExclusion() {
    final int threads = 16;
    final int iterations = 20_000;
    Mutex mutex = new Mutex();
    int[] counter = {0};
    Throwable[] error = {null};

    Thread[] workers = startWorkers(threads, () -> {
      for (int j = 0; j < iterations; j++) {
        mutex.lock();
        counter[0]++;
        mutex.unlock();
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

  private static void testOnlyOneInside() {
    Mutex mutex = new Mutex();
    int[] inside = {0};
    boolean[] bad = {false};
    Throwable[] error = {null};

    Thread[] workers = startWorkers(16, () -> {
      for (int j = 0; j < 3000; j++) {
        mutex.lock();
        inside[0]++;
        if (inside[0] != 1) {
          bad[0] = true;
        }
        inside[0]--;
        mutex.unlock();
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

  private static void testLockWaitsForUnlock() {
    Mutex mutex = new Mutex();
    mutex.lock();

    Task<Void> second = Task.start(() -> {
      mutex.lock();
      mutex.unlock();
      return null;
    });

    try {
      if (second.finished(50)) {
        throw new AssertionError("второй Lock прошёл, пока мьютекс занят");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    mutex.unlock();

    try {
      if (!second.finished(2000)) {
        throw new AssertionError("второй Lock не проснулся после Unlock");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testEveryWaiterWakesUp() {
    Mutex mutex = new Mutex();
    final int waiters = 50;
    mutex.lock();

    Throwable[] error = {null};
    Thread[] workers = startWorkers(waiters, () -> {
      mutex.lock();
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      mutex.unlock();
    }, error);

    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    mutex.unlock();

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
        throw new AssertionError("часть горутин осталась спать навсегда");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testTryLock() {
    Mutex mutex = new Mutex();
    if (!mutex.tryLock()) {
      throw new AssertionError("TryLock на свободном мьютексе должен получиться");
    }

    Task<Boolean> got = Task.start(mutex::tryLock);
    try {
      if (!got.finished(1000)) {
        throw new AssertionError("TryLock заблокировался, а не должен");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    if (got.result()) {
      throw new AssertionError("TryLock на занятом мьютексе должен вернуть false");
    }

    mutex.unlock();
    if (!mutex.tryLock()) {
      throw new AssertionError("после Unlock мьютекс свободен");
    }
    mutex.unlock();
  }

  private static void testUnlockWithoutLockPanics() {
    Mutex mutex = new Mutex();
    try {
      mutex.unlock();
    } catch (RuntimeException expected) {
      return;
    }
    throw new AssertionError("Unlock без Lock должен паниковать");
  }

  private static void testHandoffUnderLoad() {
    Mutex mutex = new Mutex();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(300);
    int[] passes = new int[8];
    Throwable[] error = {null};
    Thread[] threads = new Thread[8];

    for (int i = 0; i < threads.length; i++) {
      int id = i;
      threads[i] = new Thread(() -> {
        try {
          int count = 0;
          while (System.nanoTime() < deadline) {
            mutex.lock();
            count++;
            mutex.unlock();
          }
          passes[id] = count;
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

    for (int i = 0; i < threads.length; i++) {
      try {
        threads[i].join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (error[0] != null) {
        throw asRuntime(error[0]);
      }
      if (threads[i].isAlive()) {
        throw new AssertionError("горутина зависла");
      }
      if (passes[i] == 0) {
        throw new AssertionError("горутина не получила мьютекс ни разу");
      }
    }
  }

  private static void benchmarkUncontended(int n) {
    Mutex mutex = new Mutex();
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      mutex.lock();
      mutex.unlock();
    }
    long elapsed = System.nanoTime() - start;
    System.out.printf("BenchmarkUncontended %d ops, %.1f ns/op%n", n, (double) elapsed / n);
  }

  private static void benchmarkContended(int n) {
    runContended("BenchmarkContended", n, null);
  }

  /**
   * Аналог BenchmarkStdlibContended: в Go это sync.Mutex, здесь ReentrantLock.
   */
  private static void benchmarkStdlibContended(int n) {
    ReentrantLock lock = new ReentrantLock();
    runContended("BenchmarkStdlibContended", n, lock);
  }

  private static void runContended(String name, int n, ReentrantLock stdlib) {
    Mutex mutex = stdlib == null ? new Mutex() : null;
    int procs = Runtime.getRuntime().availableProcessors();
    int perThread = Math.max(1, n / procs);
    Thread[] threads = new Thread[procs];
    for (int i = 0; i < procs; i++) {
      threads[i] = new Thread(() -> {
        for (int j = 0; j < perThread; j++) {
          if (stdlib != null) {
            stdlib.lock();
            stdlib.unlock();
          } else {
            mutex.lock();
            mutex.unlock();
          }
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
    System.out.printf("%s %d ops, %.1f ns/op%n", name, ops, (double) elapsed / ops);
  }
}
