package com.james.pattern.decorator;

public class Coffee extends Beverage {
    public Coffee() {
        description = "Coffee";
    }

    public double cost() {
        return 0.99;
    }
}
