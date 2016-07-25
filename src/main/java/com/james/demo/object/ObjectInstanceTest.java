package com.james.demo.object;

public class ObjectInstanceTest {
    public static void main(String[] args) {
        Base a = new A();

        System.out.println(a instanceof Base);
        System.out.println(a instanceof A);
        System.out.println(a instanceof B);
    }

}

class Base {

}

class A extends Base {

}

class B extends Base {

}

class AA extends A {

}
