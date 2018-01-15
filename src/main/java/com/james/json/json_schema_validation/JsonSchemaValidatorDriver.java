package com.james.json.json_schema_validation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class JsonSchemaValidatorDriver {
    private static final String AD_TRACK_PARAM_AD_ID = "ad_id";

    private static final String PARAM_LOG_ID = "log_id";

    private static final String PARAM_LOG_CONTEXT = "log_context";

    public static String INVALID_LOGS_BUCKET = "validation_fail_logs";

    public static String REJECTED_LOGS_LABEL = "rejected_logs";

    private final static Logger LOG = Logger.getLogger(JsonSchemaValidatorDriver.class);

    public static final String CLEAN_APP_ID_PATTERN_STR = "[a-zA-Z_0-9\\-]{1,40}";

    private static Pattern CLEAN_APP_ID_PATTERN = Pattern.compile(CLEAN_APP_ID_PATTERN_STR);

    public static final int MAX_LOGRECORD_SIZE_IN_KB = 256 * 1000;

    public static void main(String[] args) throws JSONException {
        System.out.println("Json schema validation starting...");

        String jsonString = "{\"payload\":{\"duration\":1,\"route_id\":\"ceac1961-124b-491e-9ad6-0ad924234ec2\",\"distance\":0,\"caused_by\":\"EXIT\",\"log_context\":{\"log_id\":\"e722269d-c0f5-4e9a-a451-338de3e6e7d2\",\"time_zone\":\"ACT\",\"current_lat\":37.399094,\"utc_timestamp\":1508978706618,\"visitor_id\":\"b09f270c-736f-40c0-965b-f2089936f60d\",\"car_id\":\"\",\"log_version\":\"v2\",\"reg_vid\":\"DenaliMY19_Unknown\",\"current_lon\":-121.9770482},\"event_name\":\"NAV_END\",\"parent_route_id\":\"8a7da039-eb2f-421f-8c82-056f5b96c603\",\"schema_definition\":\"NavEnd\"},\"logshed_app_id\":\"denali_usage_logs_replay\",\"client_address\":\"10.222.224.172\",\"type\":1,\"slogtime\":1511287032829}";
        System.out.println("jsonString:\n\t" + jsonString);

        JSONObject logs = new JSONObject(jsonString);

        String payloadString = logs.getString("payload");
        System.out.println("payloadString=" + payloadString);

        JSONObject payload = new JSONObject(payloadString);
        String defination = payload.getString("schema_definition");
        System.out.println("defination=" + defination);

        ValidationResult result = validateLogRecord(payload);
        System.out.println(result.isSuccess());

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

            String version = "v0";
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

        // if (LOG.isDebugEnabled()) {
        // if(!result.isSuccess()) {
        // LOG.debug("Validation failed with ErrorMessage:" + result.getErrorMessage() +
        // " for payload:" + payload );
        // }
        // }

        return result;
    }

    public static ParseResult parseAndValidateJson(String jsonString) {
        Collection<LogRecord> logRecords = new ArrayList<LogRecord>();

        int validationFailedRecordsCount = 0;
        int sizeFailedRecordsCount = 0;
        StringBuilder errorMessage = new StringBuilder();
        String clientAddress = "clientAddress";
        JSONArray errorJsons = new JSONArray();

        String appId = "appId";
        // Parse the json document
        try {

            JSONObject jsonObject = new JSONObject(jsonString);
            JSONArray logs = (JSONArray) jsonObject.get("logs");

            if (logs != null && logs.length() > 0) {

                for (int i = 0; i < logs.length(); i++) {
                    JSONObject payload = (JSONObject) logs.get(i);

                    String logRecordAppId = "appId";

                    JSONObject failedJsonPayload = new JSONObject();

                    if (true) {
                        ValidationResult validationResult = validateLogRecord(payload);

                        if (!validationResult.isSuccess()) {
                            failedJsonPayload.put("logshed_app_id", appId);
                            failedJsonPayload.put("payload", payload);
                            failedJsonPayload.put("error_message", validationResult.getErrorMessage());

                            LOG.debug("JSON Validation Failed , here are details : " + failedJsonPayload);

                            logRecordAppId = INVALID_LOGS_BUCKET;
                            validationFailedRecordsCount++;

                            errorJsons.put(failedJsonPayload);

                            // errorMessage.append(failedJsonPayload.toString()).append("\n");
                        }
                    }
                    LogRecord logRecord = null;
                    if (logRecordAppId.equals(INVALID_LOGS_BUCKET)) {
                        LOG.debug("Added Validtion failed logs with the following JSON : " + failedJsonPayload);
                        logRecord = createLogRecordWithJSON(logRecordAppId, failedJsonPayload, clientAddress);
                    } else {
                        logRecord = createLogRecordWithJSON(logRecordAppId, payload, clientAddress);
                    }

                    if (!logRecordAppId.equals(REJECTED_LOGS_LABEL)) {
                        logRecords.add(logRecord);
                    } else {
                        LOG.info("Rejecting the log as the size is greater than 256KB");
                    }

                }

            }
        } catch (JSONException e) {
            errorMessage
                    .append("Got Json Exception while parsing the data. appId:" + appId + " message:" + e.getMessage());
        }

        JSONObject errorJson = new JSONObject();
        try {
            errorJson.put("error_report", errorJsons);
        } catch (JSONException e) {
            errorMessage.append("errorJson.put() error");
        }

        ParseResult parseResult = new ParseResult(logRecords, validationFailedRecordsCount, sizeFailedRecordsCount,
                errorJson);

        return parseResult;
    }
}
