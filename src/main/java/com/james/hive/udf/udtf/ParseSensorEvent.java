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

public class ParseSensorEvent extends GenericUDTF {
    private static final String NULL_STRING = "?";

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
        if (args.length != 1) {
            throw new UDFArgumentException(
                    "com.telenav.udf.parseSensorEvent.ParseSensorEvent takes exactly 1 argument");
        }

        // input
        stringOI = (PrimitiveObjectInspector) args[0];
        System.out.println("stringOI=" + stringOI);

        // output fields names
        List<String> fieldNames = new ArrayList<String>(18);
        // logId,regVid,visitorId,carId,eventName,sensorType,value,timestamp,previous_value,
        // status,type,
        // vehicle_lat,vehicle_lon,heading,elevation,speed,precision,mode
        fieldNames.add("logId");
        fieldNames.add("regVid");
        fieldNames.add("visitorId");
        fieldNames.add("carId");
        fieldNames.add("eventName");
        fieldNames.add("sensorType");
        fieldNames.add("value");
        fieldNames.add("timestamp");
        fieldNames.add("previousValue");
        fieldNames.add("status");
        fieldNames.add("type");
        fieldNames.add("vehicle_lat");
        fieldNames.add("vehicle_lon");
        fieldNames.add("heading");
        fieldNames.add("elevation");
        fieldNames.add("speed");
        fieldNames.add("precision");
        fieldNames.add("mode");

        // output fields types
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(18);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    private List<Object[]> parseSensorEvent(String rawLog) throws JSONException {
        /*
         * parse json log
         */
        List<Object[]> objectArrayList = parseJson(rawLog);

        return objectArrayList;
    }

    private List<Object[]> parseJson(String jsonString) throws JSONException {
        ArrayList<Object[]> result = new ArrayList<Object[]>();

        if (null == jsonString || jsonString.isEmpty()) {
            return result;
        }

        JSONObject logs = new JSONObject(jsonString);
        JSONObject payload = (JSONObject) logs.get("payload");

        // extract fileds for table join
        JSONObject logContext = (JSONObject) payload.get("log_context");
        String logId = logContext.getString("log_id");
        String regVid = logContext.getString("reg_vid");
        String visitorId = logContext.getString("visitor_id");
        String carId = logContext.getString("car_id");
        String eventName = payload.getString("event_name");

        for (int j = 0; j < sensorTypeArray.length; j++) {
            String sensorType = sensorTypeArray[j];
            if (!jsonString.contains(sensorType)) {
                continue;
            }

            JSONArray sensorDataArray = (JSONArray) payload.getJSONArray(sensorType);
            for (int i = 0; i < sensorDataArray.length(); i++) {
                JSONObject arrayTuple = (JSONObject) sensorDataArray.get(i);
                String value = arrayTuple.isNull("value") ? NULL_STRING : arrayTuple.getString("value");
                String timestamp = arrayTuple.isNull("timestamp") ? NULL_STRING : arrayTuple.getString("timestamp");
                String previous_value = arrayTuple.isNull("previous_value") ? NULL_STRING
                        : arrayTuple.getString("previous_value");
                String status = arrayTuple.isNull("status") ? NULL_STRING : arrayTuple.getString("status");
                String type = arrayTuple.isNull("type") ? NULL_STRING : arrayTuple.getString("type");
                String vehicle_lat = arrayTuple.isNull("vehicle_lat") ? NULL_STRING
                        : arrayTuple.getString("vehicle_lat");
                String vehicle_lon = arrayTuple.isNull("vehicle_lon") ? NULL_STRING
                        : arrayTuple.getString("vehicle_lon");
                String heading = arrayTuple.isNull("heading") ? NULL_STRING : arrayTuple.getString("heading");
                String elevation = arrayTuple.isNull("elevation") ? NULL_STRING : arrayTuple.getString("elevation");
                String speed = arrayTuple.isNull("speed") ? NULL_STRING : arrayTuple.getString("speed");
                String precision = arrayTuple.isNull("precision") ? NULL_STRING : arrayTuple.getString("precision");
                String mode = arrayTuple.isNull("mode") ? NULL_STRING : arrayTuple.getString("mode");

                result.add(new Object[] { logId, regVid, visitorId, carId, eventName, sensorType, value, timestamp,
                        previous_value, status, type, vehicle_lat, vehicle_lon, heading, elevation, speed, precision,
                        mode });
            }
        }

        return result;
    }
}
