package com.kholopova.backend.c_semaphore;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class Semaphore {
  private final AtomicInteger permits = new AtomicInteger();

  public Semaphore(int n) {
    permits.set(n);
  }

  public void acquire() {
    while (permits.get() <= 0) {
      Futex.wait(permits, 0);
    }
    permits.decrementAndGet();
  }

  public boolean tryAcquire() {
    if (permits.get() <= 0) {
      return false;
    }
    permits.decrementAndGet();
    return true;
  }

  public void release() {
    permits.incrementAndGet();
    Futex.wake(permits);
  }

  public int available() {
    return permits.get();
  }
}

