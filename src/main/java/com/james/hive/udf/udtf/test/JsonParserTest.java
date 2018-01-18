package com.james.hive.udf.udtf.test;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.james.hive.udf.udtf.FormatErrorType;

public class JsonParserTest {
    public static String[] sensorTypeArray;

    public static void main(String[] args) throws Exception {
        System.out.println("JsonParserTest starting...");

        initSensorTypeArray();

        String jsonString = "{\"payload\":{\"log_context\":{\"account_type\":\"FREE\",\"current_lat\":37.70471800895039,\"app_version\":\"1.0.45.1302\",\"device_model\":\"iPhone\",\"login_type\":\"ANONYMOUS\",\"visitor_id\":\"478ABB43-B61C-45DC-9364-529586BCEF82\",\"usage_type\":\"CLOUD\",\"app_status\":\"CONNECTED\",\"gps_state\":\"ENABLED\",\"car_id\":\"94:B2:CC:3F:FB:C0\",\"app_id\":\"3d337917-2d94-436c-a8e9-83bfdcf4b2c4\",\"reg_vid\":\"4M0ZIG1P9GYHZZUBX1ZLCJIBS\",\"current_lon\":-120.99563068690875,\"log_id\":\"A815B586-71C1-42A1-B48F-F9708F2318E3\",\"connection_type\":\"WIFI\",\"billing_id\":\"\",\"utc_timestamp\":1514744680923,\"os_version\":\"11.1.2\",\"map_source\":\"OSM\",\"offer_code\":\"\",\"time_zone\":\"America/Los_Angeles\",\"device_make\":\"Apple Inc.\",\"carrier\":\"Verizon\",\"ad_id\":\"001893A5-7B36-4425-98C0-6F721E88B4A8\",\"device_uid\":\"478ABB43-B61C-45DC-9364-529586BCEF82\",\"build\":\"1.0.45.1302\",\"device_orientation\":\"PORTRAIT\",\"os_name\":\"iOS\",\"auto_make\":\"toyota-lahaina\",\"log_version\":\"\"},\"event_name\":\"VEHICLE_SENSOR_EVENT\",\"wheel_ground_velocity\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"vehicle_speed\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"accelerator_pedal_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"steering_wheel_angle\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"torque_at_transmission\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"engine_speed\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"odometer\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"brake_pedal_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"throttle_pedal_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"light_state\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"windshield_wiper_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"transmission_gear_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"transmission_shift_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"automatic_transmission_gear_position\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"ignition_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"door_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"parking_brake_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"fuel_level\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"fuel_consumed_since_restart\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"vehicle_gps_sensor\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"vehicle_gps_sensor_precise\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"fuel_consumption_rate\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"fuel_type\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"headlamp_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"seat_belt_indication\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"seat_belt_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"lane_departure\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"tire_pressure\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"driver_workload\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"battery_state_of_charge\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"battery_charge_low\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"cruise_control_active\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"pitch_angle\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"pedestrian_warning_indication\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"outside_air_temp\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"outside_air_temp_corrected\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"compass_corrected_heading\":[],\"infotainment_system_state\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"turn_signal_status\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"headlight_setting\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"high_voltage_battery_state_of_charge\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"instantaneous_fuel_consumption_rate\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"apply_brake_pedal\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}],\"IMU_acceleration\":[{\"value\":1,\"timestamp\":1515430800,\"previous_value\":0},{\"value\":2,\"timestamp\":1515434400,\"previous_value\":1},{\"value\":3,\"timestamp\":1515438000,\"previous_value\":2}]}}";
        System.out.println("jsonString:\n\t" + jsonString);

        // JSONObject logs = new JSONObject(jsonString);
        //
        // JSONObject payload = (JSONObject) logs.get("payload");
        //
        // // JSONObject
        // // wheel_ground_velocity=(JSONObject)payload.get("wheel_ground_velocity");
        // JSONArray sensorTypeArray = (JSONArray)
        // payload.getJSONArray("wheel_ground_velocity");
        // for (int i = 0; i < sensorTypeArray.length(); i++) {
        // JSONObject arrayTuple = (JSONObject) sensorTypeArray.get(i);
        // String value = arrayTuple.getString("value");
        // String timestamp = arrayTuple.getString("timestamp");
        // String previous_value = arrayTuple.getString("previous_value");
        // System.out.println("value=" + value + "\ttimestamp=" + timestamp +
        // "\tprevious_value=" + previous_value);
        // }
        // System.out.println();

        List<Object[]> objectArrayList = parseJson(jsonString, "");
        for (Object[] objs : objectArrayList) {
            for (int i = 0; i < objs.length; i++) {
                System.out.print("\t" + (String)objs[i]);
            }
            System.out.println();
        }
    }

    private static void initSensorTypeArray() {
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

    private static List<Object[]> parseJson(String jsonString, String formatErrorType) throws JSONException {
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
}