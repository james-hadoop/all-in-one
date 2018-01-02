package com.james.demo.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTest2 {
    public static void main(String[] args) {
        String patternText1 = "[\\r\\n]";
        String patternText2 = "\\r\\n\\t\\u";

        String srcString = "I am a\n staff\r Hello.";

        System.out.println("srcString: " + srcString);
        String desString = filterReservedCharacters(srcString, patternText1);
        System.out.println("desString: " + desString);
    }

    public static String filterReservedCharacters(String srcString, String pat) {

        Pattern pattern = Pattern.compile(pat);
        Matcher matcher = pattern.matcher(srcString);

        String ret = matcher.replaceAll("");

        return ret;
    }
}
