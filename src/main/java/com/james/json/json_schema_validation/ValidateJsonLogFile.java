package com.james.json.json_schema_validation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONObject;

public class ValidateJsonLogFile {
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

    public static void main(String[] args) throws IOException {
        System.out.println("Json schema validation starting...");

        String path = "/Users/qjiang/Desktop/20180209/CONNECTIVITY_FAIL2.log";
        validateJsonLogFile(path);

        System.out.println("Json schema validation stopping...");
    }

    public static void validateJsonLogFile(String path) throws IOException {
        if (null == path || path.isEmpty()) {
            System.out.println("Wrong file name!!!");
            return;
        }

        BufferedReader br = new BufferedReader(new FileReader(path));

        String line;
        while ((line = br.readLine()) != null) {
            System.out.println(line);

            ValidationResult validateLogRecord = validateLogRecord(line);
            if (null == validateLogRecord || false == validateLogRecord.isSuccess()) {
                br.close();
                return;
            }
        }

        br.close();
    }

    public static ValidationResult validateLogRecord(String jsonString) {
        if (null == jsonString || jsonString.isEmpty()) {
            return null;
        }

        ValidationResult result = null;

        try {
            JSONObject logRecord = new JSONObject(jsonString);

            JSONObject payload = logRecord.getJSONObject("payload");

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
}
