package com.james.pattern.strategy;

public class QuackMute implements QuackBehavior {
	public void quack() {
		System.out.println("I'am slient!");
	}
}
