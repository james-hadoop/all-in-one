package com.james.interview.concurrent;

public class TestLock {
    private int i;

    private final static Object o = new Object();

    public synchronized void setI(int i) {
        this.i = i;
    }

    public int getI() {
        synchronized (o) {
            return i;
        }
    }
}
