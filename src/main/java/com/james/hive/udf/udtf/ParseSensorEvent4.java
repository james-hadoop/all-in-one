package com.james.hive.udf.udtf;

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
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class ParseSensorEvent4 extends GenericUDTF {
    private PrimitiveObjectInspector stringOI = null;
    private String[] sensorTypeArray = new String[] { "wheel_ground_velocity", "vehicle_speed",
            "accelerator_pedal_position", "steering_wheel_angle", "torque_at_transmission", "engine_speed", "odometer",
            "brake_pedal_status", "brake_pedal_position", "throttle_pedal_position", "light_state",
            "windshield_wiper_status", "transmission_gear_position", "transmission_shift_position",
            "automatic_transmission_gear_position", "ignition_status", "door_status", "parking_brake_status",
            "fuel_level", "fuel_consumed_since_restart", "fuel_consumption_rate", "fuel_type", "headlamp_status",
            "seat_belt_indication", "seat_belt_status", "lane_departure", "tire_pressure", "driver_workload",
            "battery_state_of_charge", "battery_charge_low", "cruise_control_active", "pitch_angle",
            "pedestrian_warning_indication", "outside_air_temp", "outside_air_temp_corrected",
            "compass_corrected_heading", "infotainment_system_state", "turn_signal_status", "headlight_setting",
            "high_voltage_battery_state_of_charge", "instantaneous_fuel_consumption_rate", "apply_brake_pedal",
            "IMU_acceleration" };

    @Override
    public void process(Object[] args) throws HiveException {
        final String rawLog = stringOI.getPrimitiveJavaObject(args[0]).toString();

        List<Object[]> sensorParseResultList;
        try {
            sensorParseResultList = parseSensorEvent(rawLog);
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
            throw new UDFArgumentException(
                    "com.telenav.udf.parseSensorEvent.ParseSensorEvent() takes exactly 6 argument");
        }

        // input
        stringOI = (PrimitiveObjectInspector) args[0];
        System.out.println("stringOI=" + stringOI);

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

    private List<Object[]> parseSensorEvent(String rawLog) throws JSONException {
        String formatErrorType = FormatErrorType.Good.getValue();

        /*
         * parse json log
         */
        List<Object[]> objectArrayList = parseJson(rawLog, formatErrorType);

        return objectArrayList;
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

            JSONArray sensorDataArray = (JSONArray) payload.getJSONArray(sensorType);
            for (int i = 0; i < sensorDataArray.length(); i++) {
                JSONObject arrayTuple = (JSONObject) sensorDataArray.get(i);
                String value = arrayTuple.getString("value");
                String timestamp = arrayTuple.getString("timestamp");
                String previous_value = arrayTuple.getString("previous_value");

                result.add(new Object[] { sensorType, value, timestamp, previous_value, formatErrorType });
            }
        }

        return result;
    }
}
