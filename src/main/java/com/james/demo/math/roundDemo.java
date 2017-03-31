package com.james.demo.math;

import java.text.DecimalFormat;

public class roundDemo {
    public static void main(String[] args) {
        double data = 0.077799999999999994;

        System.out.println(data);
        System.out.println(Math.round(data));
        System.out.println(customRound(data, 2));
        System.out.println(customRound(data, 3));
        System.out.println(customRound(data, 4));
        System.out.println(customRound(data, 0));
    }

    private static Double customRound(Double data, Integer decimal) {
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
