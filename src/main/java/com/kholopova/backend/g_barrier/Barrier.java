package com.kholopova.backend.g_barrier;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class Barrier {
  private final AtomicInteger need = new AtomicInteger();
  private final AtomicInteger arrived = new AtomicInteger();
  private final AtomicInteger round = new AtomicInteger();

  public Barrier(int n) {
    need.set(n);
  }

  public void await() {
    int generation = round.get();
    if (arrived.incrementAndGet() == need.get()) {
      arrived.set(0);
      round.incrementAndGet();
      Futex.wakeAll(round);
    } else {
      while (round.get() == generation) {
        Futex.wait(round, generation);
      }
    }
  }
}
