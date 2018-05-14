package com.james.demo.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogshedLogNameExtracter {
    public static void main(String[] args) {
        String logshedLogName = "logshed_denali_usage_logs_2018-04-13-10-40-GMT_ec2-logshedcollectorexternal-11-90000.gz20180511024548";

        Pattern p = Pattern.compile("(\\d{4})-(\\d{1,2})-(\\d{1,2})-(\\d{1,2})-(\\d{1,2})");
        Matcher m = p.matcher(logshedLogName);

        if (m.find()) {
            System.out.println("日期:" + m.group());
            System.out.println("年:" + m.group(1));
            System.out.println("月:" + m.group(2));
            System.out.println("日:" + m.group(3));
            System.out.println("时:" + m.group(4));
            System.out.println("分:" + m.group(5));
        }
    }
}
