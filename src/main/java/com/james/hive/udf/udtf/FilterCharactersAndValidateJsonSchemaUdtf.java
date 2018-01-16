package com.james.hive.udf.udtf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class FilterCharactersAndValidateJsonSchemaUdtf extends GenericUDTF {
    private PrimitiveObjectInspector stringOI = null;

    @Override
    public void process(Object[] args) throws HiveException {
        final String payload = stringOI.getPrimitiveJavaObject(args[0]).toString();

        List<Object[]> sensorParseResultList = validateJsonSchemaAndParse(payload);

        Iterator<Object[]> iter = sensorParseResultList.iterator();
        while (iter.hasNext()) {
            Object[] r = iter.next();
            forward(r);
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 6) {
            throw new UDFArgumentException("LogSchemaValidationAndFilterCharactersUdtf() takes exactly 6 argument");
        }

        // input
        stringOI = (PrimitiveObjectInspector) args[0];

        // output fields names
        List<String> fieldNames = new ArrayList<String>(5);
        fieldNames.add("sensorType");
        fieldNames.add("value");
        fieldNames.add("timestamp");
        fieldNames.add("previousValue");
        fieldNames.add("invalidLogFormat");

        // output fields types
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(5);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private Map<String, Object> filterCharactersIfContain(String payload) {
        Map<String, Object> resultAndContentMap = new HashMap<String, Object>();

        if (null == payload || payload.isEmpty()) {
            resultAndContentMap.put("result", FormatErrorType.Good.getValue());
            return resultAndContentMap;
        }

        Pattern CRLF = Pattern.compile("(\r\n|\r|\n|\n\r)");
        Matcher m = CRLF.matcher(payload);

        String payloadClean = "";
        if (m.find()) {
            payloadClean = m.replaceAll(" ");

            resultAndContentMap.put("result", FormatErrorType.ContainReservedCharacter.getCode());
            resultAndContentMap.put("content", payloadClean);
            return resultAndContentMap;
        } else {
            resultAndContentMap.put("result", FormatErrorType.Good.getCode());
        }

        return resultAndContentMap;
    }

    private List<Object[]> validateJsonSchemaAndParse(String payload) {
        List<Object[]> objectList = new ArrayList<Object[]>();
        
        if (payload == null || payload.isEmpty()) {
            return objectList;
        }
        
        String payloadClean=payload;
        String formatErrorType=FormatErrorType.Good.getValue();
        Map<String, Object> filterResultMap=filterCharactersIfContain(payload);
        
        int formatErrorTypeCode=(int) filterResultMap.get("result");
        if(formatErrorTypeCode!=FormatErrorType.Good.getCode()) {
            payloadClean=(String) filterResultMap.get("content");
            formatErrorType=FormatErrorType.getValue(formatErrorTypeCode);
        }
        
        
        
//        objectList.add(new Object[] { tokens[0], tokens[1] });
        
        // TODO
        return objectList;
    }
}