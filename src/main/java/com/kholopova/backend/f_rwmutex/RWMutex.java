package com.kholopova.backend.f_rwmutex;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class RWMutex {
  static final int WRITER = 1 << 31;
  static final int FREE = 0;

  private final AtomicInteger state = new AtomicInteger();

  public void rLock() {
    while (true) {
      int count = state.get();
      if (count == WRITER) {
        Futex.wait(state, count);
        continue;
      }
      if (state.compareAndSet(count, count + 1)) {
        return;
      }
    }
  }

  public void rUnlock() {
    int count = state.decrementAndGet();
    if (count < 0) {
      throw new IllegalStateException("паника");
    }
    if (count == 0) {
      Futex.wakeAll(state);
    }
  }

  public void lock() {
    while (!state.compareAndSet(FREE, WRITER)) {
      Futex.wait(state, WRITER);
    }
  }

  public void unlock() {
    int previous = state.getAndSet(FREE);
    if (previous == WRITER) {
      Futex.wakeAll(state);
    } else if (previous == FREE) {
      throw new IllegalStateException("паника");
    }
  }
}
