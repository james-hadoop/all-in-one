package com.james.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class NextMonthStartDay extends UDF {
    public Text evaluate(Text srcDateString, Text inputFormat, Text outputFormat) throws ParseException {
        if (srcDateString == null || inputFormat == null || outputFormat == null) {
            return null;
        }
        SimpleDateFormat srcFormat = new SimpleDateFormat(inputFormat.toString());
        SimpleDateFormat targetFormat = new SimpleDateFormat(outputFormat.toString());
        Date srcDate = srcFormat.parse(srcDateString.toString());
        Calendar srcCal = Calendar.getInstance();
        srcCal.setTime(srcDate);
        srcCal.set(srcCal.get(Calendar.YEAR), srcCal.get(Calendar.MONTH), 1);
        srcCal.add(Calendar.MONTH, 1);
        String ret = targetFormat.format(srcCal.getTime());

        return new Text(ret);
    }
}
