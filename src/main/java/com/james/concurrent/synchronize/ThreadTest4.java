package com.james.concurrent.synchronize;

/**
 * 被操作的Value2类 含成员变量value
 */
class Value2 {
    private int value;

    /*
     * 使value值加1
     */
    public synchronized void increaseValue2() {
        while (value != 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        ++value;
        System.out.println("increaseValue2():\t" + value);

        notify();
    }

    /*
     * 使value值减1
     */
    public synchronized void decreaseValue2() {
        while (value == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        --value;
        System.out.println("decreaseValue2():\t" + value);

        notify();
    }
}

/**
 * 
 * 是Value2类中的value值增加1的线程
 * 
 */
class IncreaseThread2 extends Thread {
    private Value2 objValue2;

    public IncreaseThread2(Value2 obj) {
        this.objValue2 = obj;
    }

    @Override
    public void run() {
        // 当Value2对象的value值为0时，使该值增加1
        for (int i = 0; i < 10; i++) {
            // Thread.sleep((long) (Math.random() * 1000));
            objValue2.increaseValue2();
        }
    }
}

/**
 * 
 * 是Value2类中的value值减小1的线程
 * 
 */
class DecreaseThread2 extends Thread {
    private Value2 objValue2;

    public DecreaseThread2(Value2 obj) {
        this.objValue2 = obj;
    }

    @Override
    public void run() {
        for (int i = 0; i < 10; i++) {
            // Thread.sleep((long) (Math.random() * 1000));
            objValue2.decreaseValue2();
        }
    }
}

public class ThreadTest4 {
    public static void main(String[] args) {
        Value2 obj = new Value2();

        // 启动增加线程
        new IncreaseThread2(obj).start();
        new IncreaseThread2(obj).start();
        // 启动减小线程
        new DecreaseThread2(obj).start();
    }
}