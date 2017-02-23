package com.james.interview.concurrent;

public class TestLockMain {
    public static void main(String[] args) {
        new Thread(new TestLockThread()).start();
        new Thread(new TestLockThread2()).start();
    }
}
