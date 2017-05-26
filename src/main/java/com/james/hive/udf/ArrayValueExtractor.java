package com.james.hive.udf;

import java.text.ParseException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class ArrayValueExtractor extends UDF {
    public Text evaluate(Text hiveArrayValue, Text key, Text divider) throws ParseException {
if(null==hiveArrayValue||hiveArrayValue.getLength()==0){
    return null;
}


        
        
        return null;
    }

}
