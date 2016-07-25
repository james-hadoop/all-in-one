package com.james.demo.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTest {
    public static void main(String[] args) {
        String strSrc1 = "-Xms1024m -Xmx4096m";
        String strSrc2 = "-Xmx4096m -Xms1024m";

        System.out.println(parseJavaEnv(strSrc1));
        System.out.println(parseJavaEnv(strSrc2));
    }

    public static String parseJavaEnv(String text) {
        String patternText1 = ".*-Xmx";
        String patternText2 = "m.*";

        Pattern pattern = Pattern.compile(patternText1);
        Matcher matcher = pattern.matcher(text);

        String ret = matcher.replaceAll("");

        pattern = Pattern.compile(patternText2);
        matcher = pattern.matcher(ret);
        ret = matcher.replaceAll("");

        return ret;
    }
}
