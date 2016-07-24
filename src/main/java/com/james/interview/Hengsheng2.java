package com.james.interview;

import com.james.common.util.JamesUtil;

public class Hengsheng2 extends Base {
    public static void main(String[] args) {
        new Hengsheng2();
        new Base();
        JamesUtil.printDivider();

        try {
            badMethod();
            System.out.print("A");
        } catch (Exception ex) {
            System.out.print("B");
        } finally {
            System.out.print("C");
        }
        System.out.print("D");
        JamesUtil.printDivider();

        new Hengsheng2().makeThings();
        JamesUtil.printDivider();

        try {
            if ((new Object()).equals((new Object()))) {
                System.out.println("equal");
            } else {
                System.out.println("not equal");
            }
        } catch (Exception e) {
            System.out.println("exception");
        }
    }

    public static void badMethod() {
    }

    void makeThings() {
        TestA test = new TestA();
    }
}

class Base {
    Base() {
        System.out.print("Base");
    }
}

class TestA {
    TestB b;

    TestA() {
        b = new TestB(this);
    }
}

class TestB {
    TestA a;

    TestB(TestA a) {
        this.a = a;
    }
}