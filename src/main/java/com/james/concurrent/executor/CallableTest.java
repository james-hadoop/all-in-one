package com.james.concurrent.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CallableTest {
    public static void main(String[] args) {
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

        List<Future<Integer>> listResult = new ArrayList<>();

        Random random = new Random();

        for (int i = 0; i < 10; i++) {
            Integer number = random.nextInt(10);

            FactorialCalculator calculator = new FactorialCalculator(number);

            Future<Integer> result = executor.submit(calculator);
            listResult.add(result);
        }

        do {
            System.out.printf("Main: Number of Completed Tasks: %d\n", executor.getCompletedTaskCount());

            for (int i = 0; i < listResult.size(); i++) {
                Future<Integer> result = listResult.get(i);
                System.out.printf("Main: Task %d: %s\n", i, result.isDone());
            }

            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        } while (executor.getCompletedTaskCount() < listResult.size());

        System.out.printf("Main: Results\n");
        for (int i = 0; i < listResult.size(); i++) {
            Future<Integer> result = listResult.get(i);
            Integer number = null;
            try {
                number = result.get();
            } catch (InterruptedException | ExecutionException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.printf("Main: Task %d: %d\n", i, number);
        }

        executor.shutdown();
    }
}

class FactorialCalculator implements Callable<Integer> {
    private Integer number;

    public FactorialCalculator(Integer number) {
        this.number = number;
    }

    @Override
    public Integer call() throws Exception {
        int result = 1;

        if ((number == 0) || number == 1) {
            result = 1;
        } else {
            for (int i = 2; i <= number; i++) {
                result *= i;
                TimeUnit.SECONDS.sleep(20);
            }
        }

        System.out.printf("%s: %d\n", Thread.currentThread().getName(), result);

        return result;
    }

}
