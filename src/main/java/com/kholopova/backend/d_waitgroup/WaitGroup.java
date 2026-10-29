package com.kholopova.backend.d_waitgroup;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class WaitGroup {
  private final AtomicInteger count = new AtomicInteger();

  public void add(int delta) {
    count.addAndGet(delta);
  }

  public void done() {
    if (count.decrementAndGet() < 0) {
      throw new IllegalStateException("паника");
    }
  }

  public void await() {
    if (count.get() != 0) {
      Futex.wait(count, 0);
      while (count.get() != 0) {
      }
      Futex.wakeAll(count);
    }
  }
}
