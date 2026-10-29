package com.kholopova.backend.b_mutex;

import com.kholopova.backend.a_spinlock.Locker;
import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class Mutex implements Locker {
  static final int FREE = 0;
  static final int HELD = 1;
  static final int CONTENDED = 2;

  private final AtomicInteger state = new AtomicInteger(FREE);

  public void lock() {
    if (!tryLock()) {
      while (state.getAndSet(CONTENDED) != FREE) {
        Futex.wait(state, CONTENDED);
      }
    }
  }

  public boolean tryLock() {
    return state.compareAndSet(FREE, HELD);
  }

  public void unlock() {
    int previous = state.getAndSet(FREE);
    if (previous == CONTENDED) {
      Futex.wake(state);
    } else if (previous == FREE) {
      throw new IllegalStateException("паника");
    }
  }
}
