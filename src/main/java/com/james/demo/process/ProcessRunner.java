package com.james.demo.process;

import java.io.IOException;

public class ProcessRunner {

    public static void main(String[] args) throws InterruptedException, IOException {
        String shellFilePath = "/home/james/logs/cp_log.sh";

        Process process = null;
        String shellCommand = "chmod 777 " + shellFilePath;
        process = Runtime.getRuntime().exec(shellCommand);
        process.waitFor();

        String shellRunCommand = "/bin/sh " + shellFilePath;
        System.out.println("run shell : " + shellRunCommand);
        int shellReturnValue = Runtime.getRuntime().exec(shellRunCommand).waitFor();
        System.out.println("shell return value: " + shellReturnValue);
        boolean ret = shellReturnValue == 0 ? true : false;
        if (ret) {
            System.out.println("SUCCEED");
        } else {
            System.out.println("FAILED");
        }
    }
}
