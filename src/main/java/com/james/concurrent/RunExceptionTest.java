package com.james.concurrent;

import java.lang.Thread.UncaughtExceptionHandler;

public class RunExceptionTest {
    public static void main(String[] args) {
        Task task = new Task();
        Thread thread = new Thread(task);
        thread.setUncaughtExceptionHandler(new ExcptionHandler());
        thread.start();
    }
}

class ExcptionHandler implements UncaughtExceptionHandler {

    public void uncaughtException(Thread t, Throwable e) {
        System.out.printf("An exception has been captured\n");
        System.out.printf("Thread: %s\n", t.getId());
        System.out.printf("Exception: %s: %s\n", e.getClass().getName(), e.getMessage());
    }
}

class Task implements Runnable {
    public void run() {
        int numero = Integer.parseInt("TTT");
    }
}
