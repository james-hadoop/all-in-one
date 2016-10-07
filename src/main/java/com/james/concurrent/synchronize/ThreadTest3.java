package com.james.concurrent.synchronize;

/**
 * 被操作的Value类 含成员变量value
 */
class Value {
    private int value = 0;

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    /*
     * 锁对象，value值增加1之后输出
     */
    public synchronized void increaseValue() {
        System.out.print("increaseValue():\t");
        System.out.println(++value);
    }

    /*
     * 锁对象，value值减小1之后输出
     */
    public synchronized void decreaseValue() {
        System.out.print("decreaseValue():\t");
        System.out.println(--value);
    }
}

/**
 * 
 * 是Value类中的value值增加1的线程
 * 
 */
class IncreaseThread extends Thread {
    private Value objValue;

    public IncreaseThread(Value obj) {
        this.objValue = obj;
    }

    @Override
    public void run() {
        while (true) {
            // 当Value对象的value值为0时，使该值增加1
            if (objValue.getValue() == 0) {
                objValue.increaseValue();
            }
        }
    }
}

/**
 * 
 * 是Value类中的value值减小1的线程
 * 
 */
class DecreaseThread extends Thread {
    private Value objValue;

    public DecreaseThread(Value obj) {
        this.objValue = obj;
    }

    @Override
    public void run() {
        while (true) {
            // 当Value对象的value值不为0时，使该值减小1
            if (objValue.getValue() != 0) {
                objValue.decreaseValue();
            }
        }
    }
}

public class ThreadTest3 {
    public static void main(String[] args) {
        Value obj = new Value();

        // 启动增加线程
        new IncreaseThread(obj).start();
        // 启动减小线程
        new DecreaseThread(obj).start();
    }
}