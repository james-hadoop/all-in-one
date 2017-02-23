package com.james.interview.concurrent;

public class TestLockThread implements Runnable {
    private TestLock lock = new TestLock();

    @Override
    public void run() {
        while (true) {
            int i = lock.getI();
            System.out.println("getI()=" + i);
        }
    }
}
