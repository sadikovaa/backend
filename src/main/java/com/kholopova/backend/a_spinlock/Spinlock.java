package com.kholopova.backend.a_spinlock;

import java.util.concurrent.atomic.AtomicBoolean;

public class Spinlock implements Locker {

  private final AtomicBoolean lock = new AtomicBoolean(false);

  @Override
  public void lock() {
    while (!tryLock()) {
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
