package com.james.demo.character.range;

import java.io.UnsupportedEncodingException;

import com.james.common.util.JamesUtil;

public class Main {
    public static void main(String args[]) throws UnsupportedEncodingException {
        String Chinese = "你好 ";
        String English = "hello ";
        String Japanese = "サインインする";
        String Italian = "saldo dell' ordine";
        String German = "des Gutscheines";

        System.out.println((int) Chinese.charAt(0));
        System.out.println((int) English.charAt(0));
        System.out.println((int) Japanese.charAt(0));
        System.out.println((int) Italian.charAt(0));
        System.out.println((int) German.charAt(0));
        JamesUtil.printDivider();

        System.out.println((Chinese.getBytes("shift-jis").length >= (2 * Chinese.length())));
        System.out.println((Japanese.getBytes("shift-jis").length >= (2 * Japanese.length())));
        System.out.println((Italian.getBytes("shift-jis").length >= (2 * Italian.length())));
    }
}
