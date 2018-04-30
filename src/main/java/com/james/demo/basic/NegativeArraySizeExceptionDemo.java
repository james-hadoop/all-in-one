package com.james.demo.basic;

import java.util.ArrayList;

public class NegativeArraySizeExceptionDemo {

    public static void main(String[] args) {
        // NegativeArraySizeException
        // int[] arr = new int[-1];

        // correct
        // int[] arr = new int[0];

        // NegativeArraySizeException
        // ArrayList<Object> ret = new ArrayList(-1);

        // correct
        // ArrayList<Object> ret = new ArrayList(0);

        // NegativeArraySizeException
        // ArrayList<Object> ret = new ArrayList(null);

        // NegativeArraySizeException
        ArrayList<Object> container = null;
        ArrayList<Object> ret = new ArrayList(container.size());
    }
}
