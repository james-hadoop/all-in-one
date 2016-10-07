package com.james.concurrent.synchronize;

public class ThreadTest1 {
    public static void main(String[] args) {
        // 通过start()方法启动线程
        new Thread1().start();
        // 通过run()方法启动线程
        new Thread2().run();
    }
}

/*
 * 继承Thread类
 */
class Thread1 extends Thread {
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println("Thread1:\t" + i);
        }
    }
}

/*
 * 实现Runnable接口
 */
class Thread2 implements Runnable {
    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.println("Thread2:\t" + i);
        }
    }
}