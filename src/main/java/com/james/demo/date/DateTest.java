package com.james.demo.date;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateTest {

    public static void main(String[] args) {
        long lData = 1460592013123L;

        System.out.println(getTimestamp(lData));
    }

    public static String getTimestamp(long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        DateFormat dirFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return dirFormat.format(cal.getTime());
    }
}
