package com.kholopova.backend.internal;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Фьютексы переписала на Java при помощи ИИ
 */
public final class Futex {
    private static final Object mu = new Object();
    private static final IdentityHashMap<AtomicInteger, ArrayDeque<Waiter>> waiters = new IdentityHashMap<>();

    private Futex() {}

    /** Аналог {@code Wait(addr, val)}. */
    public static void wait(AtomicInteger addr, int val) {
        Waiter waiter = new Waiter();
        synchronized (mu) {
            if (addr.get() != val) {
                return;
            }
            waiters.computeIfAbsent(addr, key -> new ArrayDeque<>()).addLast(waiter);
        }

        boolean interrupted = false;
        synchronized (waiter) {
            while (!waiter.signaled) {
                try {
                    waiter.wait();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /** Аналог {@code Wake(addr)}: будит одного. */
    public static void wake(AtomicInteger addr) {
        Waiter waiter;
        synchronized (mu) {
            ArrayDeque<Waiter> queue = waiters.get(addr);
            if (queue == null || queue.isEmpty()) {
                return;
            }
            waiter = queue.removeFirst();
            if (queue.isEmpty()) {
                waiters.remove(addr);
            }
        }
        signal(waiter);
    }

    /** Аналог {@code WakeAll(addr)}. */
    public static void wakeAll(AtomicInteger addr) {
        ArrayDeque<Waiter> queue;
        synchronized (mu) {
            queue = waiters.remove(addr);
        }
        if (queue == null) {
            return;
        }
        for (Waiter waiter : queue) {
            signal(waiter);
        }
    }

    private static void signal(Waiter waiter) {
        synchronized (waiter) {
            waiter.signaled = true;
            waiter.notify();
        }
    }

    /**
     * Канал емкостью 1 из Go: сигнал не теряется, даже если {@code wake}
     * успел раньше, чем ждущий дошёл до {@code wait}.
     */
    private static final class Waiter {
        private boolean signaled;
    }
}
