package com.kholopova.backend.c_semaphore;

import java.util.concurrent.atomic.AtomicInteger;
import com.kholopova.backend.internal.Futex;

public class Semaphore {
  private final AtomicInteger permits = new AtomicInteger();

  public Semaphore(int n) {
    if (n < 0) {
      throw new IllegalArgumentException("паника х2");
    }
    permits.set(n);
  }

  public void acquire() {
    while (true) {
      int current = permits.get();
      if (current <= 0) {
        Futex.wait(permits, current);
        continue;
      }
      if (permits.compareAndSet(current, current - 1)) {
        return;
      }
    }
  }

  public boolean tryAcquire() {
    while (true) {
      int current = permits.get();
      if (current <= 0) {
        return false;
      }
      if (permits.compareAndSet(current, current - 1)) {
        return true;
      }
    }
  }

  public void release() {
    while (true) {
      int current = permits.get();
      if (current == Integer.MAX_VALUE) {
        throw new IllegalStateException("паника");
      }
      if (permits.compareAndSet(current, current + 1)) {
        Futex.wake(permits);
        return;
      }
    }
  }

  public int available() {
    return permits.get();
  }
}

