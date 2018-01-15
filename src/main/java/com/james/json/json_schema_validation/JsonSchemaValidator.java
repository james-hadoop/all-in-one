package com.james.json.json_schema_validation;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.load.configuration.LoadingConfiguration;
import com.github.fge.jsonschema.load.uri.URITransformer;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.report.ProcessingMessage;
import com.github.fge.jsonschema.report.ProcessingReport;

public class JsonSchemaValidator {

    private static Map<String, JsonSchemaValidator> validators = new ConcurrentHashMap<String, JsonSchemaValidator>();

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

    private JsonSchemaFactory jsonSchemaFactory;
    private boolean isValidationEnabled;
    private String namespace;
    private String fileChecksum;

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
}
