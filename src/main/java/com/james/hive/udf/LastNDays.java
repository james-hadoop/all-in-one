package com.james.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public final class LastNDays extends UDF {
    public Text evaluate(Text srcDateString, Text inputFormat, IntWritable ndays, Text outputFormat)
            throws ParseException {
        if (srcDateString == null || inputFormat == null || outputFormat == null || ndays == null) {
            throw new NullPointerException();
        }

        SimpleDateFormat srcFormat = new SimpleDateFormat(inputFormat.toString());
        SimpleDateFormat targetFormat = new SimpleDateFormat(outputFormat.toString());
        Date srcDate = srcFormat.parse(srcDateString.toString());
        Calendar srcCal = Calendar.getInstance();
        srcCal.setTime(srcDate);
        srcCal.add(Calendar.DAY_OF_MONTH, -ndays.get());
        String ret = targetFormat.format(srcCal.getTime());

        return new Text(ret);
    }
}
