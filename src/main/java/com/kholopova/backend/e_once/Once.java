package com.kholopova.backend.e_once;

import com.kholopova.backend.internal.Futex;

import java.util.concurrent.atomic.AtomicInteger;

public class Once {
  static final int NOT_COMPLETED = 0;
  static final int IN_PROCESS = 1;
  static final int COMPLETED = 2;

  private final AtomicInteger state = new AtomicInteger(NOT_COMPLETED);


  public void run(Runnable action) {
    if (state.compareAndSet(NOT_COMPLETED, IN_PROCESS)) {
      try {
        action.run();
      } finally {
        state.set(COMPLETED);
        Futex.wakeAll(state);
      }
    } else {
      while (state.get() != COMPLETED) {
        Futex.wait(state, IN_PROCESS);
      }
    }
  }

  public boolean done() {
    return state.get() == COMPLETED;
  }
}
