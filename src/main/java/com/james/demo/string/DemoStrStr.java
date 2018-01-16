package com.james.demo.string;

public class DemoStrStr {

    public static void main(String[] args) {
        String text = "Helloel";

        String pattern1 = "el";
        String pattern2 = "Helloel";
        String pattern3 = "HelloelE";

        System.out.println(strstr(text, pattern1));
        System.out.println(strstr(text, pattern2));
        System.out.println(strstr(text, pattern3));
    }

    public static int strstr(String text, String pattern) {
        if (null == text || 0 == text.length() || null == pattern || 0 == pattern.length()
                || pattern.length() > text.length()) {
            return -1;
        }

        for (int i = 0; i < text.length(); i++) {
            int k = i;

            for (int j = 0; j < pattern.length(); j++) {
                if (pattern.charAt(j) != text.charAt(k++)) {
                    break;
                }

                if (j == pattern.length() - 1) {
                    return i;
                }
            }
        }

        return -1;
    }
}