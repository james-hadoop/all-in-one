package com.james.concurrent.executor;

import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class ScheduledTaskTest {
    public static void main(String[] args) {
        /*
         * Creates a thread pool that can schedule commands to run after a given delay,
         * or to execute periodically.
         */
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        System.out.printf("Main: Starting at: %s\n", new Date());

        ScheduledTask task = new ScheduledTask("Task");

        /*
         * Creates and executes a periodic action that becomes enabled first after the
         * given initial delay, and subsequently with the given period; that is
         * executions will commence after initialDelay then initialDelay+period, then
         * initialDelay + 2 * period, and so on. If any execution of the task encounters
         * an exception, subsequent executions are suppressed. Otherwise, the task will
         * only terminate via cancellation or termination of the executor. If any
         * execution of this task takes longer than its period, then subsequent
         * executions may start late, but will not concurrently execute.
         */
        ScheduledFuture<?> result = executor.scheduleAtFixedRate(task, 1, 2, TimeUnit.SECONDS);

        for (int i = 0; i < 10; i++) {
            // System.out.printf("Main: Delay: %d\n",
            // result.getDelay(TimeUnit.MILLISECONDS));
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        executor.shutdown();
        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
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
