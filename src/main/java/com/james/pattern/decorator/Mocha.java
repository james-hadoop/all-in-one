package com.james.pattern.decorator;

public class Mocha extends CondimentDecorator {
	Beverage beverage;

	public Mocha(Beverage beverage) {
		this.beverage = beverage;
	}

	public String getDecription() {
		return beverage.getDecription() + ": Mocha";
	}

	public double cost() {
		return beverage.cost() + 0.5;
	}
}
