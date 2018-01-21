package com.james.concurrent;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ThreadFactoryTest {
    public static void main(String[] args) {
        MyThreadFactory factory = new MyThreadFactory("MyThreadFactory");
        ThreadFactoryTask task = new ThreadFactoryTask();

        Thread thread = null;
        System.out.printf("Starting the Threads\n");

        for (int i = 0; i < 10; i++) {
            /*
             * Constructs a new Thread. Implementations may also initialize priority, name,
             * daemon status, ThreadGroup, etc.
             */
            thread = factory.newThread(task);
            thread.start();
        }

        System.out.printf("Factory stats:\n");
        System.out.printf("%s\n", factory.getStats());
    }
}

class MyThreadFactory implements ThreadFactory {
    private int counter;
    private String name;
    private List<String> stats;

    public MyThreadFactory(String name) {
        counter = 0;
        this.name = name;
        stats = new ArrayList<String>();
    }

    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, name + "=Thread_" + counter);
        counter++;
        stats.add(String.format("Created thread %d with name %s on %s\n", t.getId(), t.getName(), new Date()));
        return t;
    }

    public String getStats() {
        StringBuffer buffer = new StringBuffer();
        Iterator<String> iter = stats.iterator();

        while (iter.hasNext()) {
            buffer.append(iter.next());
            buffer.append("\n");
        }

        return buffer.toString();
    }
}

class ThreadFactoryTask implements Runnable {
    public void run() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
