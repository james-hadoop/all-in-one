package com.james.common.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StringUtil {
    private static final String KEY_DIVIDER = "\\|";
    private static final String KEY_DIVIDER_FOR_AGGREGATION = "|";
    private static final String KEY_VALUE_DIVIDER = ":";

    public static Map<String, Object> string2Map(String text) {
        if (null == text || 0 == text.length()) {
            return null;
        }

        Map<String, Object> map = new HashMap<String, Object>();

        String[] arrKeys = text.split(KEY_DIVIDER);

        for (String keys : arrKeys) {
            String[] arrKV = keys.split(KEY_VALUE_DIVIDER);

            if (2 == arrKV.length) {
                map.put(arrKV[0], arrKV[1]);
            } else {
                map.put(arrKV[0], "");
            }
        }

        return map;
    }

    public static String string2Map(Map<String, Object> map) {
        if (null == map || 0 == map.size()) {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        Set<String> setKey = map.keySet();
        for (String key : setKey) {
            sb.append(key + KEY_VALUE_DIVIDER + map.get(key) + KEY_DIVIDER_FOR_AGGREGATION);
        }

        return sb.substring(0, sb.length() - 1).toString();
    }

    public static void main(String[] args) {
        String t1 = "k1:v1";
        String t2 = "k1:v1|k2:v2";
        String t3 = "k1:v1|k2:v2|k3:v3";
        String t4 = "";

        Map<String, Object> map = new HashMap<String, Object>();
        String text;
        
        map = StringUtil.string2Map(t1);
        JamesUtil.printMap(map);
        text=string2Map(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t2);
        JamesUtil.printMap(map);
        text=string2Map(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t3);
        JamesUtil.printMap(map);
        text=string2Map(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t4);
        JamesUtil.printMap(map);
        text=string2Map(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();
    }
}
