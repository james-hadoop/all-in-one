package com.james.pattern.factory;

public class Main {
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		PizzaStore nyStore = new NYPizzaStore();

		Pizza pizza = nyStore.orderPizza("cheese");
	}
}
