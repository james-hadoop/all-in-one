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

    public static String map2String(Map<String, Object> map) {
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

    public static byte[] long2ByteArray(long l) {
        byte[] array = new byte[8];
        int i;
        int shift;
        for (i = 0, shift = 56; i < 8; i++, shift -= 8) {
            array[i] = (byte) (0xFF & (l >> shift));
        }
        return array;
    }

    public static byte[] int2ByteArray(int value) {
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            int offset = (3 - i) * 8;
            b[i] = (byte) ((value >>> offset) & 0xFF);
        }
        return b;
    }

    public static void putIntInByteArray(int value, byte[] buf, int offset) {
        for (int i = 0; i < 4; i++) {
            int valueOffset = (3 - i) * 8;
            buf[offset + i] = (byte) ((value >>> valueOffset) & 0xFF);
        }
    }

    public static int byteArray2Int(byte[] b) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }
        return value;
    }

    public static long byteArray2Long(byte[] b) {
        int value = 0;
        for (int i = 0; i < 8; i++) {
            int shift = (8 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }
        return value;
    }

    public static boolean hasBinaryContent(String contentType) {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

        return typeStr.contains("image") || typeStr.contains("audio") || typeStr.contains("video") || typeStr.contains("application");
    }

    public static boolean hasPlainTextContent(String contentType) {
        String typeStr = (contentType != null) ? contentType.toLowerCase() : "";

        return typeStr.contains("text") && !typeStr.contains("html");
    }

    public static String multiSpace2SingleSpace(String str) {
        if (null == str || 0 == str.length()) {
            return str;
        }

        return str.replaceAll("[' ']+", " ");
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
        text = map2String(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t2);
        JamesUtil.printMap(map);
        text = map2String(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t3);
        JamesUtil.printMap(map);
        text = map2String(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        map = StringUtil.string2Map(t4);
        JamesUtil.printMap(map);
        text = map2String(map);
        System.out.println(text);
        map = StringUtil.string2Map(text);
        JamesUtil.printMap(map);
        JamesUtil.printDivider();

        String[] arr1 = t4.split("|");
        System.out.println(arr1.length);
        System.out.println(arr1[0]);
        JamesUtil.printDivider();

        String strMultiSpace = "a  b c   d";
        String strSingleSpace = strMultiSpace.replaceAll("[' ']+", " ");
        System.out.println("strMultiSpace=" + strMultiSpace + "\t" + "strSingleSpace=" + strSingleSpace);
        JamesUtil.printDivider();
    }
}
