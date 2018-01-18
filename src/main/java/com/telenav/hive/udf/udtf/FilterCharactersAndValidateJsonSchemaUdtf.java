package com.telenav.hive.udf.udtf;

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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.james.json.json_schema_validation.ValidationResult;

public class FilterCharactersAndValidateJsonSchemaUdtf extends GenericUDTF {
    private PrimitiveObjectInspector stringOI = null;
    private String[] sensorTypeArray;

    @Override
    public void process(Object[] args) throws HiveException {
        final String payload = stringOI.getPrimitiveJavaObject(args[0]).toString();

        List<Object[]> sensorParseResultList;
        try {
            sensorParseResultList = validateJsonSchemaAndParse(payload);
        } catch (JSONException e) {
            throw new HiveException(e.getMessage());
        }

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

        // sensorType name
        initSensorTypeArray();

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

    private void initSensorTypeArray() {
        sensorTypeArray = new String[] { "wheel_ground_velocity", "vehicle_speed", "accelerator_pedal_position",
                "steering_wheel_angle", "torque_at_transmission", "engine_speed", "odometer", "brake_pedal_status",
                "brake_pedal_position", "throttle_pedal_position", "light_state", "windshield_wiper_status",
                "transmission_gear_position", "transmission_shift_position", "automatic_transmission_gear_position",
                "ignition_status", "door_status", "parking_brake_status", "fuel_level", "fuel_consumed_since_restart",
                "fuel_consumption_rate", "fuel_type", "headlamp_status", "seat_belt_indication", "seat_belt_status",
                "lane_departure", "tire_pressure", "driver_workload", "battery_state_of_charge", "battery_charge_low",
                "cruise_control_active", "pitch_angle", "pedestrian_warning_indication", "outside_air_temp",
                "outside_air_temp_corrected", "compass_corrected_heading", "infotainment_system_state",
                "turn_signal_status", "headlight_setting", "high_voltage_battery_state_of_charge",
                "instantaneous_fuel_consumption_rate", "apply_brake_pedal", "IMU_acceleration" };
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

    private List<Object[]> parseJson(String jsonString, String formatErrorType) throws JSONException {
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        if (null == jsonString || jsonString.isEmpty()) {
            return result;
        }

        if (null == formatErrorType || formatErrorType.isEmpty()) {
            formatErrorType = FormatErrorType.InvalidJsonSchema.getValue();
        }

        JSONObject logs = new JSONObject(jsonString);
        JSONObject payload = (JSONObject) logs.get("payload");

        for (int j = 0; j < sensorTypeArray.length; j++) {
            String sensorType = sensorTypeArray[j];
            if (!jsonString.contains(sensorType)) {
                continue;
            }

            JSONArray sensorTypeArray = (JSONArray) payload.getJSONArray(sensorType);
            for (int i = 0; i < sensorTypeArray.length(); i++) {
                JSONObject arrayTuple = (JSONObject) sensorTypeArray.get(i);
                String value = arrayTuple.getString("value");
                String timestamp = arrayTuple.getString("timestamp");
                String previous_value = arrayTuple.getString("previous_value");

                result.add(new Object[] { sensorType, value, timestamp, previous_value, formatErrorType });
            }
        }

        return result;
    }

    private List<Object[]> validateJsonSchemaAndParse(String jsonString) throws JSONException {
        List<Object[]> objectList = new ArrayList<Object[]>();

        if (jsonString == null || jsonString.isEmpty()) {
            return objectList;
        }

        /*
         * filter special characters if containing
         */
        String jsonStringClean = jsonString;
        String formatErrorType = FormatErrorType.Good.getValue();
        Map<String, Object> filterResultMap = filterCharactersIfContain(jsonString);

        int formatErrorTypeCode = (int) filterResultMap.get("result");
        if (formatErrorTypeCode != FormatErrorType.Good.getCode()) {
            jsonStringClean = (String) filterResultMap.get("content");
            formatErrorType = FormatErrorType.getValue(formatErrorTypeCode);
        }

        /*
         * validate if match json schema
         */
        boolean isValidateJson = validateLogRecord(jsonStringClean);
        if (false == isValidateJson) {
            formatErrorType = FormatErrorType.InvalidJsonSchema.getValue();
        }

        /*
         * parse json log
         */
        List<Object[]> objectArrayList = parseJson(jsonString, formatErrorType);

        return objectArrayList;
    }
}