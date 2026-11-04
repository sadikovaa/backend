package com.kholopova.backend.concurrency.a_spinlock;

public interface Locker {
  void lock();

  void unlock();

  boolean tryLock();
}
