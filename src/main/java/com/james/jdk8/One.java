package com.james.jdk8;

public class One {
    public static void main(String[] args) {
        Formula formula = new Formula() {
            public double calculate(int a) {
                return sqrt(a * 100);
            }
        };

        System.out.println(formula.calculate(100)); // 100.0
        System.out.println(formula.sqrt(16)); // 4.0
    }
}

interface Formula {
    double calculate(int a);

    default double sqrt(int a) {
        return Math.sqrt(a);
    }
}
