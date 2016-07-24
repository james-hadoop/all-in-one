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

        ExecutorService executor = (ExecutorService) Executors.newCachedThreadPool();
        String result;

        try {
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
