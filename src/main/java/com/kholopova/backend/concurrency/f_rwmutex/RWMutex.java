package com.kholopova.backend.concurrency.f_rwmutex;

import java.util.concurrent.atomic.AtomicInteger;
import com.kholopova.backend.concurrency.internal.Futex;

public class RWMutex {
  static final int WRITER = 1 << 31;
  static final int FREE = 0;

  private final AtomicInteger state = new AtomicInteger();
  private final AtomicInteger waitingWriters = new AtomicInteger();

  public void rLock() {
    while (true) {
      int current = state.get();
      if ((current & WRITER) != 0 || waitingWriters.get() > 0) {
        Futex.wait(state, current);
        continue;
      }
      if (current == Integer.MAX_VALUE) {
        throw new IllegalStateException("паника");
      }
      if (!state.compareAndSet(current, current + 1)) {
        continue;
      }
      if (waitingWriters.get() == 0) {
        return;
      }
      if (state.decrementAndGet() == 0) {
        Futex.wakeAll(state);
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
    boolean inWaitingWriters = false;
    while (true) {
      int current = state.get();
      if (current == FREE && state.compareAndSet(FREE, WRITER)) {
        if (inWaitingWriters) {
          waitingWriters.decrementAndGet();
        }
        return;
      }
      if (!inWaitingWriters) {
        waitingWriters.incrementAndGet();
        inWaitingWriters = true;
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
