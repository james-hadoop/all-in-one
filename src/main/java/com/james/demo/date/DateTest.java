package com.james.demo.date;

import java.text.SimpleDateFormat;

public class DateTest {

    public static void main(String[] args) {
        final String strDate = "1460592013000";
        long lDate = Long.parseLong(strDate);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String ret = sdf.format(lDate);
        System.out.println(ret);
        
        String ret1=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Long.parseLong(strDate));
        System.out.println(ret1);
        System.out.println("\n\n");
    }
}
