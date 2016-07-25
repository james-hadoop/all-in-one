package com.james.demo.encryption;

public class Test {

    public static void main(String[] args) {
        String world = "hello_james";

        String encode = DataHelper.getMD5Str(world).toLowerCase();

        System.out.println("encode: " + encode);
    }
}
