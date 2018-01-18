package com.james.hive.udf.udtf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.load.configuration.LoadingConfiguration;
import com.github.fge.jsonschema.load.uri.URITransformer;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;
import com.james.json.json_schema_validation.LogRecord;
import com.james.json.json_schema_validation.ParseResult;
import com.james.json.json_schema_validation.ValidationResult;

public class JsonSchemaValidator {
    public static final String AD_TRACK_PARAM_AD_ID = "ad_id";

    public static final String PARAM_LOG_ID = "log_id";

    public static final String PARAM_LOG_CONTEXT = "log_context";

    public static String INVALID_LOGS_BUCKET = "validation_fail_logs";

    public static String REJECTED_LOGS_LABEL = "rejected_logs";

    public final static Logger LOG = Logger.getLogger(JsonSchemaValidator.class);

    public static final String CLEAN_APP_ID_PATTERN_STR = "[a-zA-Z_0-9\\-]{1,40}";

    public static Pattern CLEAN_APP_ID_PATTERN = Pattern.compile(CLEAN_APP_ID_PATTERN_STR);

    public static final int MAX_LOGRECORD_SIZE_IN_KB = 256 * 1000;

    public static Map<String, JsonSchemaValidator> validators = new ConcurrentHashMap<String, JsonSchemaValidator>();

    public static JsonSchemaValidator getRegisteredValidator(final String version, final String defination) {
        JsonSchemaValidator returnValue = null;

        if (validators.containsKey(version + defination)) {
            returnValue = validators.get(version + defination);
        }

        return returnValue;
    }

    public static JsonSchemaValidator getInstance(final String version, final String defination) {

        JsonSchemaValidator returnValue;

        if (validators.containsKey(version + defination)) {
            returnValue = validators.get(version + defination);
        } else {
            returnValue = new JsonSchemaValidator(version, defination);
            validators.put(version + defination, returnValue);
        }

        return returnValue;
    }

    public JsonSchemaFactory jsonSchemaFactory;
    public boolean isValidationEnabled;
    public String namespace;
    public String fileChecksum;

    private JsonSchemaValidator(final String version, final String defination) {
        this.namespace = null;
        this.fileChecksum = null;
        this.isValidationEnabled = false;

        initialize();
    }

    private void initialize() throws IllegalArgumentException {

        this.isValidationEnabled = true;

        final String namespaceConfig = "http://resourcedev.telenav.com/";
        final String fileChecksumConfig = "0";

        if ((namespaceConfig != null) && (fileChecksumConfig != null)) {

            if (!namespaceConfig.equals(this.namespace) || !fileChecksumConfig.equals(this.fileChecksum)) {
                this.namespace = namespaceConfig;
                this.fileChecksum = fileChecksumConfig;

                final URITransformer transformer = URITransformer.newBuilder().setNamespace(this.namespace).freeze();
                final LoadingConfiguration cfg = LoadingConfiguration.newBuilder().setURITransformer(transformer)
                        .freeze();

                this.jsonSchemaFactory = JsonSchemaFactory.newBuilder().setLoadingConfiguration(cfg).freeze();
            }
        } else {
            throw new IllegalArgumentException("Json Schema Validator loading error!");
        }
    }

    public ValidationResult validate(final String jsonInput, final String serviceName, final String schemaDefination,
            final String version) throws Exception {

        boolean success = false;
        String errorMessage = null;
        try {

            initialize(); // Check every time if there is any change in configuration
            String schemaUri = "/resources/schema/" + "analytics/" + serviceName + "/" + version + "/"
                    + schemaDefination + ".json";
            if (this.isValidationEnabled) {

                final JsonNode input = JsonLoader.fromString(jsonInput);

                final JsonSchema schema = this.jsonSchemaFactory.getJsonSchema(schemaUri);

                final ProcessingReport report = schema.validate(input);

                if (report.isSuccess()) {
                    success = true;
                } else {

                    final StringBuilder sb = new StringBuilder();
                    sb.append(schemaDefination + "version" + version).append(" : ");
                    final String newline = System.getProperty("line.separator");

                    final Iterator<ProcessingMessage> badMsgs = report.iterator();

                    while (badMsgs.hasNext()) {

                        final ProcessingMessage msg = badMsgs.next();
                        sb.append(msg.getLogLevel()).append(" : ").append(msg.getMessage()).append(newline);
                    }

                    errorMessage = sb.toString();
                    success = false;
                }
            } else {
                success = true;
            }
        } catch (final Throwable e) {
            throw new Exception(e.getMessage(), e);
        }

        ValidationResult result = new ValidationResult();
        result.setSuccess(success);
        result.setErrorMessage(errorMessage);

        return result;
    }

    public static LogRecord createLogRecordWithString(String appid, String payload, String clientAddress) {
        return new LogRecord(appid, System.currentTimeMillis(), payload, clientAddress);
    }

    public static LogRecord createLogRecordWithJSON(String appid, JSONObject payload, String clientAddress) {
        return new LogRecord(appid, System.currentTimeMillis(), payload, clientAddress);
    }

    public static ValidationResult validateLogRecord(JSONObject payload) {
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
