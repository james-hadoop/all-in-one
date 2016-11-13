package com.james.pattern.iterator;

public class Main {
    public static void main(String[] args) {

        Aggregate agg = new ConcreteAggregate();
        agg.add("abc");
        agg.add("ABC");
        agg.add("123");

        Iterator iterator = agg.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
    }
}
