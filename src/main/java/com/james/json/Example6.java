package com.james.json;

import java.io.IOException;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.load.configuration.LoadingConfiguration;
import com.github.fge.jsonschema.core.load.configuration.LoadingConfigurationBuilder;
import com.github.fge.jsonschema.core.load.uri.URITranslatorConfiguration;
import com.github.fge.jsonschema.core.ref.JsonRef;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.examples.Example1;
import com.github.fge.jsonschema.examples.Utils;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;

/**
 * Sixth example: URI redirection
 *
 * <p>
 * <a href="doc-files/Example6.java">link to source code</a>
 * </p>
 *
 * <p>
 * In this example, the same schema file is used as in {@link Example1}. This
 * time, though, it is assumed that the base URI used for addressing this schema
 * is {@code http://my.site/schemas/fstab.json#}. But instead of trying to fetch
 * it from the web directly, we want to use the local copy, which is located
 * under URI {@code
 * resource:/org/eel/kitchen/jsonschema/examples/fstab.json#}.
 * </p>
 *
 * <p>
 * The solution is here again to build a custom {@link LoadingConfiguration},
 * which allows to add schema redirections (using
 * {@link LoadingConfigurationBuilder#addSchemaRedirect(String, String)}. This
 * method can be called for as many schemas as you wish to redirect.
 * </p>
 *
 * <p>
 * The effect is that if you required a schema via URI {@code
 * http://my.site/schemas/fstab.json#}, it will silently transform this URI into
 * {@code resource:/org/eel/kitchen/jsonschema/examples/fstab.json#} internally.
 * </p>
 *
 * <p>
 * Note that URIs must be absolute JSON references (see {@link JsonRef}).
 * </p>
 */
public final class Example6 {
    private static final String FROM = "http://my.site/schemas/fstab.json#";
    private static final String TO = "resource:/com/github/fge/jsonschema/examples/fstab.json#";

    public static void main(final String... args) throws IOException, ProcessingException {
        final JsonNode good = Utils.loadResource("/fstab-good.json");
        final JsonNode bad = Utils.loadResource("/fstab-bad.json");
        final JsonNode bad2 = Utils.loadResource("/fstab-bad2.json");

        final URITranslatorConfiguration translatorCfg = URITranslatorConfiguration.newBuilder().addSchemaRedirect(FROM, TO).freeze();
        final LoadingConfiguration cfg = LoadingConfiguration.newBuilder().setURITranslatorConfiguration(translatorCfg).freeze();

        final JsonSchemaFactory factory = JsonSchemaFactory.newBuilder().setLoadingConfiguration(cfg).freeze();

        final JsonSchema schema = factory.getJsonSchema(FROM);

        ProcessingReport report;

        report = schema.validate(good);
        System.out.println(report);

        report = schema.validate(bad);
        System.out.println(report);

        report = schema.validate(bad2);
        System.out.println(report);
    }
}