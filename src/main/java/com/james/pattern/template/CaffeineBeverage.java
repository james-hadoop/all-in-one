package com.james.pattern.template;

public abstract class CaffeineBeverage {
	final void prepareRecipe(){
		boilWater();
		brew();
		pourInCup();
		addCondiments();
	}
	
	abstract void brew();
	
	abstract void addCondiments();
	
	void boilWater(){
		System.out.println("Boilling water");
	}
	
	void pourInCup(){
		System.out.println("Pouring into cup");
	}
}
