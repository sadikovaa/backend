package com.kholopova.backend.f_rwmutex;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class RWMutex {
  static final int WRITER = 1 << 31;
  static final int FREE = 0;

  private final AtomicInteger state = new AtomicInteger();

  public void rLock() {
    while (true) {
      int current = state.get();
      if ((current & WRITER) != 0) {
        Futex.wait(state, current);
        continue;
      }
      if (state.compareAndSet(current, current + 1)) {
        return;
      }
    }
  }

  public void rUnlock() {
    while (true) {
      int current = state.get();
      if ((current & WRITER) != 0 || current == FREE) {
        throw new IllegalStateException("паника");
      }
      if (state.compareAndSet(current, current - 1)) {
        if (current == 1) {
          Futex.wakeAll(state);
        }
        return;
      }
    }
  }

  public void lock() {
    while (true) {
      int current = state.get();
      if (current == FREE) {
        if (state.compareAndSet(FREE, WRITER)) {
          return;
        }
        continue;
      }
      Futex.wait(state, current);
    }
  }

  public void unlock() {
    if (!state.compareAndSet(WRITER, FREE)) {
      throw new IllegalStateException("паника");
    }
    Futex.wakeAll(state);
  }
}
