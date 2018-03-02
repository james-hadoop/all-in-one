package com.james.json.json_schema_validation;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;

public class ValidateJsonSchema extends GenericUDTF {
    private PrimitiveObjectInspector stringOI = null;

    @Override
    public void process(Object[] args) throws HiveException {
        final String rawLog = stringOI.getPrimitiveJavaObject(args[0]).toString();

        boolean isValid = validateLogRecord(rawLog);
        if (!isValid) {
            ArrayList<Object[]> results = recordInvalidRawlog(rawLog);
            Iterator<Object[]> it = results.iterator();

            while (it.hasNext()) {
                Object[] r = it.next();
                forward(r);
            }
        }
    }

    @Override
    public void close() throws HiveException {
        // do nothing

    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentException("ValidateJsonSchema() takes exactly 1 argument");
        }

        // input
        stringOI = (PrimitiveObjectInspector) args[0];

        // output fields names
        List<String> fieldNames = new ArrayList<String>(1);
        fieldNames.add("invalidRawLog");

        // output fields types
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(1);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private ArrayList<Object[]> recordInvalidRawlog(String rawLog) {
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        if (null == rawLog || rawLog.isEmpty()) {
            return result;
        }

        result.add(new Object[] { rawLog });
        return result;
    }

    private boolean validateLogRecord(String jsonString) {
        try {
            boolean result = false;

            if (null == jsonString || jsonString.isEmpty()) {
                return result;
            }

            JSONObject logs = new JSONObject(jsonString);
            JSONObject payload = (JSONObject) logs.get("payload");

            ValidationResult validationResult = JsonSchemaValidator.validateLogRecord(payload);
            result = validationResult.isSuccess();

            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
