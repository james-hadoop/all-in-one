package com.james.hive.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class DateTransfer extends UDF {
    public Text evaluate(Text srcDateString, Text srcFormatString, Text targetFormatString) throws ParseException {
        if (srcDateString == null || srcFormatString == null || targetFormatString == null) {
            return null;
        }
        SimpleDateFormat srcFormat = new SimpleDateFormat(srcFormatString.toString());
        SimpleDateFormat targetFormat = new SimpleDateFormat(targetFormatString.toString());
        Date srcDate = srcFormat.parse(srcDateString.toString());
        String ret = targetFormat.format(srcDate);

        return new Text(ret);
    }
}
