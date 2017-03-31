package com.james.hive.udf;

import java.text.DecimalFormat;
import java.text.ParseException;

import org.apache.hadoop.hive.ql.exec.UDF;

public final class CustomerRound extends UDF {
    public Double evaluate(Double data, Integer decimal) throws ParseException {

        if (null == data || null == decimal) {
            return 0.0;
        }

        String pattern = "#,#0.000#";

        switch (decimal) {
        case 2:
            pattern = "#,#0.0#";
            break;
        case 3:
            pattern = "#,#0.00#";
            break;
        }

        DecimalFormat df = new DecimalFormat(pattern);

        return new Double(Double.parseDouble(df.format(data)));
    }
}
