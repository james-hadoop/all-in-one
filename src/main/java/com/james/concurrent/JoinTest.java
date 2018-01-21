package com.james.concurrent;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class JoinTest {
    public static void main(String[] args) throws InterruptedException {
        DataSourcesLoader dsLoader1 = new DataSourcesLoader();
        Thread thread1 = new Thread(dsLoader1, "DataSourceThread");

        DataSourcesLoader dsLoader2 = new DataSourcesLoader();
        Thread thread2 = new Thread(dsLoader2, "DataSourceThread");

        thread1.start();
        thread2.start();

        /*
         * main thread will continue to execute after thread1 and thread2's finish
         * executing.
         */
        thread1.join();
        thread2.join();

        System.out.printf("Main: Configuration has been loaded: %s\n", new Date());
    }
}

class DataSourcesLoader implements Runnable {
    public void run() {
        System.out.printf("%s begin data sources loading: %s\n", Thread.currentThread().getName(), new Date());

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("%s finish data sources loading: %s\n", Thread.currentThread().getName(), new Date());
    }
}
