package com.kholopova.backend.d_waitgroup;

import java.util.concurrent.atomic.AtomicInteger;
import com.kholopova.backend.internal.Futex;

public class WaitGroup {
  private final AtomicInteger count = new AtomicInteger();

  public void add(int delta) {
    while (true) {
      int current = count.get();
      long next = (long) current + delta;
      if (next < 0 || next > Integer.MAX_VALUE) {
        throw new IllegalStateException("паника");
      }
      if (count.compareAndSet(current, (int) next)) {
        if (next == 0) {
          Futex.wakeAll(count);
        }
        return;
      }
    }
  }

  public void done() {
    add(-1);
  }

  public void await() {
    while (true) {
      int current = count.get();
      if (current == 0) {
        return;
      }
      Futex.wait(count, current);
    }
  }
}
