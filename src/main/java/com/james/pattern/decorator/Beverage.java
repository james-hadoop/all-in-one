package com.james.pattern.decorator;

public abstract class Beverage {
    String description = "Unknown Beverage";

    public String getDecription() {
        return description;
    }

    public abstract double cost();
}
