package com.james.hive.udf;

import java.text.ParseException;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpecialCharacterFilter extends UDF {
    public Text evaluate(Text srcString, Text pat) throws ParseException {
        Pattern pattern = Pattern.compile(pat.toString());
        Matcher matcher = pattern.matcher(srcString.toString());

        String ret = matcher.replaceAll("");

        return new Text(ret);
    }
}