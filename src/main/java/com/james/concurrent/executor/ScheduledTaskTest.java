package com.james.concurrent.executor;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledTaskTest {
    public static void main(String[] args) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.printf("Main: Starting at: %s\n", new Date());

        ScheduledTask task = new ScheduledTask("Task");

        ScheduledFuture<?> result = executor.scheduleAtFixedRate(task, 1, 2, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            //System.out.printf("Main: Delay: %d\n", result.getDelay(TimeUnit.MILLISECONDS));
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        executor.shutdown();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.printf("Main: Finished at: %s\n", new Date());
    }
}

class ScheduledTask implements Runnable {
    private String name;

    public ScheduledTask(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        System.out.printf("%s: Starting at : %s\n", name, new Date());
    }
}
