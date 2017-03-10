package com.james.jdk8;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Two {
    public static void main(String[] args) {
        List<String> names = Arrays.asList("peter", "anna", "mike", "xenia");

        Collections.sort(names, (String a, String b) -> b.compareTo(a));

        for (String name : names) {
            System.out.println(name);
        }
    }
}
