package com.james.demo.regex;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BracketContentExtractor {

    public static void main(String[] args) {

        String msg = "PerformanceManager[第1个中括号]Product[第2个中括号]<[第3个中括号]79~";
        List<String> list = extractMessageByRegular(msg);
        for (int i = 0; i < list.size(); i++) {
            System.out.println(i + "-->" + list.get(i));
        }
    }

    /**
     * 使用正则表达式提取中括号中的内容
     * 
     * @param msg
     * @return
     */
    public static List<String> extractMessageByRegular(String msg) {

        List<String> list = new ArrayList<String>();
        Pattern p = Pattern.compile("(\\[[^\\]]*\\])");
        Matcher m = p.matcher(msg);
        while (m.find()) {
            list.add(m.group().substring(1, m.group().length() - 1));
        }
        return list;
    }
}
