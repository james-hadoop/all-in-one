package com.james.demo.math;

public class Test {

    public static void main(String[] args) {
        int topNum = 5;

        int num1 = Integer.valueOf(String.valueOf(Math.round(topNum * 0.4)));
        System.out.println("num1=" + num1);

        int num2 = topNum - num1;
        System.out.println("num2=" + num2);

    }

}
