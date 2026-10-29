package com.kholopova.backend.d_waitgroup;

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
public class WaitGroupTest {
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

  private record Case(String name, long timeoutMs, Runnable body) {
  }

  private record Result(String name, boolean passed, String message) {
  }

  private static List<Case> cases() {
    List<Case> list = new ArrayList<>();
    list.add(new Case("TestWaitOnZeroReturnsAtOnce", 3_000, WaitGroupTest::testWaitOnZeroReturnsAtOnce));
    list.add(new Case("TestWaitsForEveryone", 8_000, WaitGroupTest::testWaitsForEveryone));
    list.add(new Case("TestManyWaiters", 8_000, WaitGroupTest::testManyWaiters));
    list.add(new Case("TestWaitBlocksUntilDone", 5_000, WaitGroupTest::testWaitBlocksUntilDone));
    list.add(new Case("TestReuse", 15_000, WaitGroupTest::testReuse));
    list.add(new Case("TestNegativeCounterPanics", 3_000, WaitGroupTest::testNegativeCounterPanics));
    list.add(new Case("TestStress", 60_000, WaitGroupTest::testStress));
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

  private static void testWaitOnZeroReturnsAtOnce() {
    WaitGroup group = new WaitGroup();
    Task<Void> waiting = Task.start(() -> {
      group.await();
      return null;
    });
    try {
      if (!waiting.finished(1000)) {
        throw new AssertionError("Wait на нулевом счётчике не должен блокировать");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testWaitsForEveryone() {
    WaitGroup group = new WaitGroup();
    final int workers = 20;
    AtomicInteger done = new AtomicInteger();
    group.add(workers);

    Throwable[] error = {null};
    Thread[] threads = startWorkers(workers, () -> {
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      done.incrementAndGet();
      group.done();
    }, error);

    Task<Void> waiting = Task.start(() -> {
      group.await();
      return null;
    });
    try {
      if (!waiting.finished(5000)) {
        throw new AssertionError("Wait не дождался");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    try {
      awaitWorkers(threads, error);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    int got = done.get();
    if (got != workers) {
      throw new AssertionError("Wait вернулся, когда закончили только " + got + " из " + workers);
    }
  }

  private static void testManyWaiters() {
    WaitGroup group = new WaitGroup();
    group.add(1);

    Throwable[] error = {null};
    Thread[] threads = startWorkers(30, group::await, error);

    try {
      Thread.sleep(50);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    group.done();

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
        throw new AssertionError("не все ожидающие проснулись");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testWaitBlocksUntilDone() {
    WaitGroup group = new WaitGroup();
    group.add(1);

    Task<Void> waiting = Task.start(() -> {
      group.await();
      return null;
    });
    try {
      if (waiting.finished(50)) {
        throw new AssertionError("Wait вернулся раньше времени");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }

    group.done();

    try {
      if (!waiting.finished(2000)) {
        throw new AssertionError("Wait не проснулся после Done");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private static void testReuse() {
    WaitGroup group = new WaitGroup();
    for (int round = 0; round < 5; round++) {
      group.add(4);
      Throwable[] error = {null};
      Thread[] threads = startWorkers(4, group::done, error);
      Task<Void> waiting = Task.start(() -> {
        group.await();
        return null;
      });
      try {
        if (!waiting.finished(2000)) {
          throw new AssertionError("раунд " + round + ": Wait завис");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      try {
        awaitWorkers(threads, error);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  private static void testNegativeCounterPanics() {
    WaitGroup group = new WaitGroup();
    try {
      group.done();
    } catch (RuntimeException expected) {
      return;
    }
    throw new AssertionError("уход счётчика в минус должен паниковать");
  }

  private static void testStress() {
    for (int round = 0; round < 200; round++) {
      WaitGroup group = new WaitGroup();
      AtomicInteger counter = new AtomicInteger();
      group.add(8);

      Throwable[] error = {null};
      Thread[] threads = startWorkers(8, () -> {
        counter.incrementAndGet();
        group.done();
      }, error);

      Task<Void> waiting = Task.start(() -> {
        group.await();
        return null;
      });
      try {
        if (!waiting.finished(5000)) {
          throw new AssertionError("раунд " + round + " завис");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      try {
        awaitWorkers(threads, error);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      int got = counter.get();
      if (got != 8) {
        throw new AssertionError("раунд " + round + ": счётчик " + got);
      }
    }
  }
}
