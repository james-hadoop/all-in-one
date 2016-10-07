package com.james.concurrent.synchronize;

public class ThreadTest2 {
    public static int value = 0;

    public static void main(String[] args) {
        new Thread3().start();
        new Thread4().start();
    }
}

class Thread3 extends Thread {
    @Override
    public void run() {
        while (true) {
            if (    ThreadTest2.value == 0) {
                ThreadTest2.value++;
                System.out.println("Thread1:\t" + ThreadTest2.value);
            }
        }
    }
}

class Thread4 extends Thread {
    @Override
    public void run() {
        while (true) {
            if (ThreadTest2.value != 0) {
                ThreadTest2.value--;
                System.out.println("Thread2:\t" + ThreadTest2.value);
            }
        }
    }
}