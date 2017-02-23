package com.james.interview.basic;

public class Sample {
    static private Integer intg = new Integer(30);

    static public void main(String[] args) {
        Sample s = new Sample();
        Integer intg = new Integer(10);
        Integer intg1 = new Integer(10);
        int i = 5;
        System.out.println(intg + " " + intg1 + " " + i);
        s.test(intg, intg1, i);
        System.out.println(intg + " " + intg1 + " " + i);
    }

    private void test(Integer intg, Integer intg1, int i) {
        intg = new Integer(intg.intValue() + 10);
        intg1 = Sample.intg;
        i += 10;
    }
}
