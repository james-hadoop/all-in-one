package com.james.demo.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FilterSpecialCharacters {
    public static void main(String[] args) {
        String srcString = "Hello! \rThis is james. \nNice to meet you! \r\nByebye!!! \n\rIt's you again";
        // String srcString = "Hello! This is james. Nice to meet you!";
        System.out.println("srcString:\n" + srcString);

        Pattern CRLF = Pattern.compile("(\r\n|\r|\n|\n\r)");
        Matcher m = CRLF.matcher(srcString);

        System.out.println("------------------------------------\n");
        String desString = "";
        if (m.find()) {
            System.out.println("isContain=" + m.find());
            desString = m.replaceAll(" ");
        } else {
            desString = srcString;
        }

        System.out.println("desString:\n" + desString);
    }
}