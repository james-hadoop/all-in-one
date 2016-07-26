package com.james.common.util;

import java.text.DecimalFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DataUtil {
    public static boolean isNullOrEmpty(String input) {
        if (input == null || input.length() <= 0) {
            return true;
        }
        return false;
    }
    
    public static boolean isNumber(String str) {
        boolean ret = false;
        if (isNullOrEmpty(str)) {
            ret = false;
        } else {
            Pattern pattern = Pattern.compile("[0-9]*");
            ret = pattern.matcher(str).matches();
        }
        return ret;
    }
    
    public static boolean isContainChinese(String str) {
        Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }

    public static Double getDoubleWith2AfterPoint(Double input) {
        Double ret = 0d;
        try {
            DecimalFormat df = new DecimalFormat("#.00");
            ret = Double.valueOf(df.format(input));
        } catch (Exception e) {
        }
        return ret;
    }

    public static Double getDoubleWith4AfterPoint(Double input) {
        Double ret = 0d;
        try {
            DecimalFormat df = new DecimalFormat("#.0000");
            ret = Double.valueOf(df.format(input));
        } catch (Exception e) {
        }
        return ret;
    }
}
