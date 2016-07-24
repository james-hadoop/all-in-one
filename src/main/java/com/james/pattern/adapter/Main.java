package com.james.pattern.adapter;

public class Main {
    public static void main(String[] args) {
        MallardDuck duck = new MallardDuck();

        WildTurkey turkey = new WildTurkey();

        Duck turkeyAdapter = new TurkeyAdapter(turkey);

        execute(duck);
        System.out.println();
        execute(turkeyAdapter);
    }

    static void execute(Duck duck) {
        duck.quack();
        duck.fly();
    }
}
