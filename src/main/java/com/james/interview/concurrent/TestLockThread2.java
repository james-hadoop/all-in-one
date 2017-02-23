package com.james.interview.concurrent;

public class TestLockThread2 implements Runnable {
    private TestLock lock = new TestLock();

    private int i = 0;

    @Override
    public void run() {
        while (true) {
            lock.setI(i++);
            System.out.println("setI()=" + i);
        }
    }
}
