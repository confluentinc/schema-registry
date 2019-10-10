package io.confluent.kafka.schemaregistry.maven;

import org.apache.avro.Schema;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public class SchemasWithDependenciesTest extends SchemaRegistryTest {

    private final String dependency = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.pizza\",\n" +
            "  \"name\": \"Amount\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"amount\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"currency\",\n" +
            "      \"type\": \"string\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private final String schema = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.pizza\",\n" +
            "  \"name\": \"Pizza\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"cost\",\n" +
            "      \"type\": \"com.pizza.Amount\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";


    @Test
    public void testSchemaWithDependenciesRegister() throws IOException {
        RegisterSchemaRegistryMojo schemaRegistryMojo = new RegisterSchemaRegistryMojo() {
            @Override
            public void execute() throws MojoExecutionException, MojoFailureException {

            }
        };

        File pizzaFile = new File(tempDirectory, "pizza.avsc");
        File amountFile = new File(tempDirectory, "amount.avsc");
        try (
                FileWriter pizzaWriter = new FileWriter(pizzaFile);
                FileWriter amountWriter = new FileWriter(amountFile)
        ) {
            pizzaWriter.write(schema);
            amountWriter.write(dependency);
        }

        ArrayList<String> imports = new ArrayList<>();
        imports.add(amountFile.getAbsolutePath());
        schemaRegistryMojo.imports = imports;

        Map<String,File> schemas = new HashMap<>();
        schemas.put("Pizza", pizzaFile);

        Map<String, ParsedSchema> parsedSchemas = schemaRegistryMojo.loadSchemas(schemas);
        Schema pizza = ((AvroSchema) parsedSchemas.get("Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }

    @Test
    public void testSchemaWithDependencies() throws IOException {
        TestCompatibilitySchemaRegistryMojo schemaRegistryMojo = new TestCompatibilitySchemaRegistryMojo() {
            @Override
            public void execute() throws MojoExecutionException, MojoFailureException {

            }


        };

        File pizzaFile = new File(tempDirectory, "pizza.avsc");
        File amountFile = new File(tempDirectory, "amount.avsc");
        try (
                FileWriter pizzaWriter = new FileWriter(pizzaFile);
                FileWriter amountWriter = new FileWriter(amountFile)
        ) {
            pizzaWriter.write(schema);
            amountWriter.write(dependency);
        }

        ArrayList<String> imports = new ArrayList<>();
        imports.add(amountFile.getAbsolutePath());
        schemaRegistryMojo.imports = imports;

        Map<String,File> schemas = new HashMap<>();
        schemas.put("Pizza", pizzaFile);

        Map<String, ParsedSchema> parsedSchemas = schemaRegistryMojo.loadSchemas(schemas);
        Schema pizza = ((AvroSchema) parsedSchemas.get("Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }
}
