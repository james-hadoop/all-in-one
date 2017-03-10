package com.james.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureTest {
    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {
        ExecutorService executor = Executors.newFixedThreadPool(1);

        Callable<String> task = new FutureTask();

        Future<String> futureResult = (Future<String>) executor.submit(task);

        // String result = futureResult.get();
        String result = futureResult.get(1, TimeUnit.SECONDS);
        System.out.println("result: " + result);

        executor.shutdown();
    }

}

class FutureTask implements Callable<String> {
    public String call() throws Exception {
        TimeUnit.SECONDS.sleep(5);
        return "Hello James";
    }
}