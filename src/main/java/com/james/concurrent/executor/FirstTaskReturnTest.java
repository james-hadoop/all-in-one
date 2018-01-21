package com.james.concurrent.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FirstTaskReturnTest {
    public static void main(String[] args) {
        String username = "test";
        String password = "test";

        UserValidator ldapUserValidator = new UserValidator("LDAP");
        UserValidator dbUserValidator = new UserValidator("DATABASE");

        TaskValidator ldapTask = new TaskValidator(ldapUserValidator, username, password);
        TaskValidator dbTask = new TaskValidator(dbUserValidator, username, password);

        List<TaskValidator> listTask = new ArrayList<>();
        listTask.add(ldapTask);
        listTask.add(dbTask);

        /*
         * Creates a thread pool that creates new threads as needed, but will reuse
         * previously constructed threads when they are available. These pools will
         * typically improve the performance of programs that execute many short-lived
         * asynchronous tasks. Calls to execute will reuse previously constructed
         * threads if available. If no existing thread is available, a new thread will
         * be created and added to the pool. Threads that have not been used for sixty
         * seconds are terminated and removed from the cache. Thus, a pool that remains
         * idle for long enough will not consume any resources. Note that pools with
         * similar properties but different details (for example, timeout parameters)
         * may be created using ThreadPoolExecutor constructors.
         */
        ExecutorService executor = (ExecutorService) Executors.newCachedThreadPool();
        String result;

        try {
            /*
             * Executes the given tasks, returning the result of one that has completed
             * successfully (i.e., without throwing an exception), if any do. Upon normal or
             * exceptional return, tasks that have not completed are cancelled. The results
             * of this method are undefined if the given collection is modified while this
             * operation is in progress.
             */
            result = executor.invokeAny(listTask);
            System.out.printf("Main: Result: %s\n", result);
        } catch (Exception e) {
            e.printStackTrace();
        }

        executor.shutdown();
        System.out.printf("Main: End of the Execution\n");
    }
}

class UserValidator {
    private String name;

    public UserValidator(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public boolean validate(String name, String password) {
        Random random = new Random();

        long duration = (long) (Math.random() * 10);
        System.out.printf("Validator %s: Validating a user during 5d seconds\n", this.name, duration);

        try {
            TimeUnit.SECONDS.sleep(duration);
        } catch (InterruptedException e) {
            return false;
        }

        return random.nextBoolean();
    }
}

class TaskValidator implements Callable<String> {
    private UserValidator validator;
    private String user;
    private String password;

    public TaskValidator(UserValidator validator, String user, String password) {
        this.validator = validator;
        this.user = user;
        this.password = password;
    }

    @Override
    public String call() throws Exception {
        if (!validator.validate(user, password)) {
            System.out.printf("%s: The user has not been found\n", validator.getName());
            throw new Exception("Error validating user");
        }

        System.out.printf("%s: The user has been found\n", validator.getName());
        return validator.getName();
    }
}
