package com.james.pattern.factory;

public class NYStylePizza extends Pizza {
	public void prepare() {
		System.out.println("New York Style prepare()");
	}

	public void bake() {
		System.out.println("New York Style bake()");
	}

	public void cut() {
		System.out.println("New York Style cut()");
	}

	public void box() {
		System.out.println("New York Style box()");
	}
}
