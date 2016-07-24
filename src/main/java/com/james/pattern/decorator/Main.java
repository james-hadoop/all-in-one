package com.james.pattern.decorator;

public class Main {
	public static void main(String[] args) {
		Coffee coffee = new Coffee();
		System.out.println(coffee.getDecription());
		System.out.println(coffee.cost() + "\n\n");

		Mocha mocha = new Mocha(coffee);
		System.out.println(mocha.getDecription());
		System.out.println(mocha.cost());
	}
}
