package io.confluent.kafka.schemaregistry.maven;

import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.maven.UploadSchemaRegistryMojo.Reference;
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
    public void testSchemaWithDependencies() throws Exception {
        RegisterSchemaRegistryMojo schemaRegistryMojo = new RegisterSchemaRegistryMojo();
        schemaRegistryMojo.client = new MockSchemaRegistryClient();

        File pizzaFile = new File(tempDirectory, "pizza.avsc");
        File amountFile = new File(tempDirectory, "amount.avsc");
        try (
                FileWriter pizzaWriter = new FileWriter(pizzaFile);
                FileWriter amountWriter = new FileWriter(amountFile)
        ) {
            pizzaWriter.write(schema);
            amountWriter.write(dependency);
        }

        Map<String, List<Reference>> refs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        subjectRefs.add(new Reference("com.pizza.Amount", "Amount", null));
        refs.put("Pizza", subjectRefs);
        schemaRegistryMojo.references = refs;

        Map<String, File> schemas = new LinkedHashMap<>();
        schemas.put("Amount", amountFile);
        schemas.put("Pizza", pizzaFile);
        schemaRegistryMojo.subjects = schemas;

        schemaRegistryMojo.execute();
        Schema pizza = ((AvroSchema) schemaRegistryMojo.schemas.get("Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }
}
