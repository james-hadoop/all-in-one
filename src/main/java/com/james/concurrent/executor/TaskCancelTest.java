package com.james.concurrent.executor;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TaskCancelTest {
    public static void main(String[] args) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();

        CancelTask task = new CancelTask();
        System.out.printf("Main: Executing the CancelTask\n");

        Future<String> result = executor.submit(task);

        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("Main: Canceling the CancelTask\n");

        /*
         * Attempts to cancel execution of this task. This attempt will fail if the task
         * has already completed, has already been cancelled, or could not be cancelled
         * for some other reason. If successful, and this task has not started when
         * cancel is called, this task should never run. If the task has already
         * started, then the mayInterruptIfRunning parameter determines whether the
         * thread executing this task should be interrupted in an attempt to stop the
         * task.
         * 
         * After this method returns, subsequent calls to isDone will always return
         * true. Subsequent calls to isCancelled will always return true if this method
         * returned true.
         */
        result.cancel(true);

        System.out.printf("Main: Cancelled: %s\n", result.isCancelled());
        System.out.printf("Main: Done %s\n", result.isDone());

        executor.shutdown();
        System.out.printf("Main: The executor has finished\n");
    }
}

class CancelTask implements Callable<String> {
    @Override
    public String call() throws Exception {
        int i = 0;
        while (true) {
            System.out.println(i++ + " round");
            Thread.sleep(100);
        }
    }
}