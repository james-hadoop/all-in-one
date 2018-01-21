package com.james.concurrent;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ReadWriteLockTest {
    public static void main(String[] args) {
        PricesInfo priceInfo = new PricesInfo();

        ReadTask[] readers = new ReadTask[5];
        Thread[] threadsReader = new Thread[5];
        for (int i = 0; i < 5; i++) {
            readers[i] = new ReadTask(priceInfo);
            threadsReader[i] = new Thread(readers[i]);
        }

        WriteTask writer = new WriteTask(priceInfo);
        Thread threadWriter = new Thread(writer);

        for (int i = 0; i < 5; i++) {
            threadsReader[i].start();
        }

        threadWriter.start();
    }
}

class PricesInfo {
    private double price1;
    private double price2;

    /*
     * A ReadWriteLock maintains a pair of associated locks, one for read-only
     * operations and one for writing. The read lock may be held simultaneously by
     * multiple reader threads, so long as there are no writers. The write lock is
     * exclusive.
     */
    private ReadWriteLock lock;

    public PricesInfo() {
        price1 = 1.0;
        price2 = 2.0;
        lock = new ReentrantReadWriteLock();
    }

    public double getPrice1() {
        lock.readLock().lock();
        double value = price1;
        lock.readLock().unlock();
        return value;
    }

    public double getPrice2() {
        lock.readLock().lock();
        double value = price2;
        lock.readLock().unlock();
        return value;
    }

    public void setPrices(double price1, double price2) {
        lock.writeLock().lock();
        this.price1 = price1;
        this.price2 = price2;
        lock.writeLock().unlock();
    }
}

class ReadTask implements Runnable {
    private PricesInfo priceInfo;

    public ReadTask(PricesInfo priceInfo) {
        this.priceInfo = priceInfo;
    }

    public void run() {
        for (int i = 0; i < 10; i++) {
            System.out.printf("%s: Price 1: %f\n", Thread.currentThread().getName(), priceInfo.getPrice1());
            System.out.printf("%s: Price 2: %f\n", Thread.currentThread().getName(), priceInfo.getPrice2());
        }
    }
}

class WriteTask implements Runnable {
    private PricesInfo priceInfo;

    public WriteTask(PricesInfo priceInfo) {
        this.priceInfo = priceInfo;
    }

    public void run() {
        for (int i = 0; i < 3; i++) {
            System.out.printf("Writer: Attempt to modify the prices.\n");
            priceInfo.setPrices(Math.random() * 10, Math.random() * 8);
            System.out.printf("Writer: Prices have been modified.\n");
            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
