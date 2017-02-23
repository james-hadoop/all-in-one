package com.james.interview.compile;

public class ClassA {
    private int i;

    // ClassB will not compile if the default constructor ClassA() is not
    // defined
    public ClassA() {

    }

    public ClassA(int i) {
        this.i = i;
    }
}
