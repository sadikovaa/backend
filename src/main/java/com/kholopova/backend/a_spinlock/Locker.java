package com.kholopova.backend.a_spinlock;

public interface Locker {
  void lock();

  void unlock();

  boolean tryLock();
}
