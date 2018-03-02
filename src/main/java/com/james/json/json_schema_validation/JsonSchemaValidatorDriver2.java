package com.james.json.json_schema_validation;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonSchemaValidatorDriver2 {
    private static final String NULL_STRING = "?";

    private static String[] sensorTypeArray = new String[] { "wheel_ground_velocity", "vehicle_speed",
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

    private static final String AD_TRACK_PARAM_AD_ID = "ad_id";

    private static final String PARAM_LOG_ID = "log_id";

    private static final String PARAM_LOG_CONTEXT = "log_context";

    public static String INVALID_LOGS_BUCKET = "validation_fail_logs";

    public static String REJECTED_LOGS_LABEL = "rejected_logs";

    private final static Logger LOG = Logger.getLogger(JsonSchemaValidatorDriver2.class);

    public static final String CLEAN_APP_ID_PATTERN_STR = "[a-zA-Z_0-9\\-]{1,40}";

    private static Pattern CLEAN_APP_ID_PATTERN = Pattern.compile(CLEAN_APP_ID_PATTERN_STR);

    public static final int MAX_LOGRECORD_SIZE_IN_KB = 256 * 1000;

    public static void main(String[] args) throws JSONException {
        System.out.println("Json schema validation starting...");

        String jsonString ="{\"payload\":{\"schema_definition\":\"VehicleSensorEvent\",\"log_context\":{\"account_type\":\"FREE\",\"current_lat\":37.70471800895039,\"app_version\":\"1.0.45.1302\",\"device_model\":\"iPhone\",\"login_type\":\"ANONYMOUS\",\"visitor_id\":\"478ABB43-B61C-45DC-9364-529586BCEF82\",\"usage_type\":\"CLOUD\",\"app_status\":\"CONNECTED\",\"gps_state\":\"ENABLED\",\"car_id\":\"94:B2:CC:3F:FB:C0\",\"app_id\":\"3d337917-2d94-436c-a8e9-83bfdcf4b2c4\",\"reg_vid\":\"4M0ZIG1P9GYHZZUBX1ZLCJIBS\",\"current_lon\":-120.99563068690875,\"log_id\":\"A815B586-71C1-42A1-B48F-F9708F2318E3\",\"connection_type\":\"WIFI\",\"billing_id\":\"\",\"utc_timestamp\":1514744680923,\"os_version\":\"11.1.2\",\"map_source\":\"OSM\",\"offer_code\":\"\",\"time_zone\":\"America/Los_Angeles\",\"device_make\":\"Appl Inc.\",\"carrier\":\"Verizon\",\"ad_id\":\"001893A5-7B36-4425-98C0-6F721E88B4A8\",\"device_uid\":\"478ABB43-B61C-45DC-9364-529586BCEF82\",\"build\":\"1.0.45.1302\",\"device_orientation\":\"PORTRAIT\",\"os_name\":\"iOS\",\"auto_make\":\"toyota-lahaina\",\"log_version\":\"\"},\"event_name\":\"VEHICLE_SENSOR_EVENT\",\"wheel_ground_velocity\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"vehicle_speed\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"accelerator_pedal_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"steering_wheel_angle\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"torque_at_transmission\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"engine_speed\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"odometer\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"brake_pedal_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"throttle_pedal_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"light_state\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"windshield_wiper_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"transmission_gear_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"transmission_shift_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"automatic_transmission_gear_position\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"ignition_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"door_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"parking_brake_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"fuel_level\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"fuel_consumed_since_restart\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"vehicle_gps_sensor\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"vehicle_gps_sensor_precise\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"fuel_consumption_rate\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"fuel_type\":[{\"value\":100,\"timestamp\":1517946089}],\"headlamp_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"seat_belt_indication\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"seat_belt_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"lane_departure\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"tire_pressure\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"driver_workload\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"battery_state_of_charge\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"battery_charge_low\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"cruise_control_active\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"pitch_angle\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"pedestrian_warning_indication\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"outside_air_temp\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"outside_air_temp_corrected\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"compass_corrected_heading\":[],\"infotainment_system_state\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"turn_signal_status\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"headlight_setting\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"high_voltage_battery_state_of_charge\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"instantaneous_fuel_consumption_rate\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"apply_brake_pedal\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}],\"IMU_acceleration\":[{\"value\":100,\"timestamp\":1517946089,\"previous_value\":99}]}}";
        //String jsonString = "{\"payload\":{\"log_context\":9,\"event_name\":\"CONNECTIVITY_FAIL\",\"log_version\":\"v2\"},\"logshed_app_id\":\"denali_usage_logs\",\"client_address\":\"10.191.1.59, 54.208.95.0, 10.191.102.238,10.191.102.45\",\"type\":1,\"slogtime\":1517857715492}";
        System.out.println("jsonString:\n\t" + jsonString);

        JSONObject logs = new JSONObject(jsonString);

        String payloadString = logs.getString("payload");
        System.out.println("payloadString=" + payloadString);

        JSONObject payload = new JSONObject(payloadString);
        String defination = payload.getString("schema_definition");
        System.out.println("defination=" + defination);

         // validate json schema
         ValidationResult result = validateLogRecord(payload);
         System.out.println(result.isSuccess());

//        // parse sensor event
//        List<Object[]> objectArrayList = parseJson(jsonString);
//        for (int i = 0; i < objectArrayList.size(); i++) {
//            Object[] objArray = objectArrayList.get(i);
//
//            for (int j = 0; j < objArray.length; j++) {
//                System.out.println(objArray[j]);
//            }
//            System.out.println("---------------------------------------");
//        }

        System.out.println("Json schema validation stopping...");
    }

    public static LogRecord createLogRecordWithString(String appid, String payload, String clientAddress) {
        return new LogRecord(appid, System.currentTimeMillis(), payload, clientAddress);
    }

    public static LogRecord createLogRecordWithJSON(String appid, JSONObject payload, String clientAddress) {
        return new LogRecord(appid, System.currentTimeMillis(), payload, clientAddress);
    }

    private static ValidationResult validateLogRecord(JSONObject payload) {

        ValidationResult result = null;

        try {

            String version = "v2";
            String defination = payload.getString("schema_definition");
            String serviceId = payload.optString("service_id");
            String serviceName = "client";

            if (serviceId.length() > 0) {
                serviceName = "service"; // if serviceId is available ,then
                                         // It belong to serviceLog
                version = payload.getString("version");
            } else {
                serviceName = "client";
                JSONObject logContext = payload.getJSONObject(PARAM_LOG_CONTEXT);
                version = logContext.getString("log_version");
            }

            JsonSchemaValidator validator = JsonSchemaValidator.getInstance(version, defination);

            if (validator != null) {
                result = validator.validate(payload.toString(), serviceName, defination, version);
            }

        } catch (Exception exception) {
            result = new ValidationResult();
            result.setSuccess(false);
            result.setErrorMessage(exception.getMessage());
        }

        return result;
    }

    private static List<Object[]> parseJson(String jsonString) throws JSONException {
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
        if (!eventName.equals("VEHICLE_SENSOR_EVENT")) {
            return result;
        }

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
