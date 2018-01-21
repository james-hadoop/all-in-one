package com.james.concurrent.assist;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SemaphoreTest {
    public static void main(String[] args) {
        PrintQueue printQueue = new PrintQueue();

        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(new PrintJob(printQueue), "Thread " + i);
        }

        for (int i = 0; i < 10; i++) {
            threads[i].start();
        }
    }
}

class PrintQueue {
    /*
     * A counting semaphore. Conceptually, a semaphore maintains a set of permits.
     * Each acquire blocks if necessary until a permit is available, and then takes
     * it. Each release adds a permit, potentially releasing a blocking acquirer.
     * However, no actual permit objects are used; the Semaphore just keeps a count
     * of the number available and acts accordingly.
     */
    private final Semaphore semaphore;
    private boolean[] freePrinters;
    private Lock lockPrinters;

    public PrintQueue() {
        semaphore = new Semaphore(3);

        freePrinters = new boolean[3];
        for (int i = 0; i < 3; i++) {
            freePrinters[i] = true;
        }

        lockPrinters = new ReentrantLock();
    }

    public void printJob(Object document) {
        try {
            /*
             * Acquires a permit from this semaphore, blocking until one is available, or
             * the thread is interrupted.
             */
            semaphore.acquire();

            int assignedPrinter = getPrinter();

            long duration = (long) (Math.random() * 10);
            System.out.printf("%s: PrintQueue: Printing a Job in Printer %d during %d seconds\n",
                    Thread.currentThread().getName(), assignedPrinter, duration);
            TimeUnit.SECONDS.sleep(duration);

            freePrinters[assignedPrinter] = true;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            /*
             * Releases a permit, returning it to the semaphore.
             */
            semaphore.release();
        }
    }

    private int getPrinter() {
        int ret = -1;

        try {
            /*
             * Acquires the lock.
             * 
             * If the lock is not available then the current thread becomes disabled for
             * thread scheduling purposes and lies dormant until the lock has been acquired.
             */
            lockPrinters.lock();

            for (int i = 0; i < freePrinters.length; i++) {
                if (freePrinters[i]) {
                    ret = i;
                    freePrinters[i] = false;
                    break;
                } // if
            } // for
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            /*
             * Releases the lock.
             */
            lockPrinters.unlock();
        }

        return ret;
    }
}

class PrintJob implements Runnable {
    private PrintQueue printQueue;

    public PrintJob(PrintQueue printQueue) {
        this.printQueue = printQueue;
    }

    public void run() {
        System.out.printf("%s: Going to print a job\n", Thread.currentThread().getName());
        printQueue.printJob(new Object());
        System.out.printf("%s: The document has been printed\n", Thread.currentThread().getName());
    }
}