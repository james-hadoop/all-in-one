package com.james.demo.regex;

public class CharacterContainTest {
    public static void main(String[] args) {
        String str = "sdfsdfx";
        String str2 = "sdfsf123z";
        System.out.println(str.matches("[a-zA-Z]*"));
        System.out.println(str2.matches("[a-zA-Z]*"));

        System.out.println(str.endsWith("x"));
        System.out.println(str2.endsWith("x"));
    }
}
