package com.james.concurrent;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class WaitTest {
    public static void main(String[] args) {
        EventStorage storage = new EventStorage();

        Producer producer = new Producer(storage);
        Thread thread1 = new Thread(producer);

        Consumer consumer = new Consumer(storage);
        Thread thread2 = new Thread(consumer);

        thread1.start();
        thread2.start();
    }
}

class EventStorage {
    private int maxSize;
    private List<Date> storage;

    public EventStorage() {
        maxSize = 10;
        storage = new LinkedList<Date>();
    }

    public synchronized void set() {
        while (storage.size() == maxSize) {
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        storage.add(new Date());
        System.out.printf("Set: %d\n", storage.size());
        notifyAll();
    }

    public synchronized void get() {
        while (storage.size() == 0) {
            try {
                wait();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        System.out.printf("Get: %d: %s\n", storage.size(), ((LinkedList<?>) storage).poll());
        notifyAll();
    }
}

class Producer implements Runnable {
    private EventStorage storage;

    public Producer(EventStorage storage) {
        this.storage = storage;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            storage.set();
        }
    }
}

class Consumer implements Runnable {
    private EventStorage storage;

    public Consumer(EventStorage storage) {
        this.storage = storage;
    }

    public void run() {
        for (int i = 0; i < 100; i++) {
            storage.get();
        }
    }
}
