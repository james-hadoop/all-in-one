package com.james.pattern.strategy;

public class Main {
	public static void main(String[] args) {
		ConcreteDuck duck = new ConcreteDuck();

		duck.setFlyBehavior(new FlyWithWings());
		duck.setQuackBehavior(new QuackMute());

		duck.performFly();
		duck.performQuack();

		duck.swim();

		duck.display();
	}
}
