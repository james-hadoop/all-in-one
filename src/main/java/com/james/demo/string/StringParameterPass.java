package com.james.demo.string;

public class StringParameterPass {

    public static void main(String[] args) {
        String srcString = "hello";
        changeValue(srcString);
        System.out.println(srcString);
        
        StringBuilder sb=new StringBuilder(srcString);
        System.out.println(sb);
    }

    private static void changeValue(String srcString) {
        srcString = "changed";
    }
    
    private static void changeValue2(StringBuilder srcString) {
        srcString.insert(0, "changed");
    }
}
