package com.kholopova.backend.a_spinlock;

import java.util.concurrent.atomic.AtomicBoolean;

public class TTAS implements Locker {
  private final AtomicBoolean lock = new AtomicBoolean(false);

  @Override
  public void lock() {
    while (true) {
      while (lock.get()) {
      }
      if (tryLock()) {
        return;
      }
    }

  }

  @Override
  public void unlock() {
    if (!lock.compareAndSet(true, false)) {
      throw new IllegalStateException("паника");
    }
  }

  @Override
  public boolean tryLock() {
    return lock.compareAndSet(false, true);
  }
}
