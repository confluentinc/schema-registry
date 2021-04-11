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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public class SchemasWithDependenciesTest extends SchemaRegistryTest {

    private final String dependency = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.shared\",\n" +
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
            "      \"type\": \"com.shared.Amount\"\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private final String combinedSchema = "[\"com.pizza.Pizza\", \"com.soda.Soda\"]";

    private final String inlineSchema1 = "{\n" +
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
            "      \"type\": {\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.shared\",\n" +
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
            "}" +
            "    }\n" +
            "  ]\n" +
            "}";

    private final String inlineSchema2 = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.soda\",\n" +
            "  \"name\": \"Soda\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"cost\",\n" +
            "      \"type\": {\n" +
            "  \"type\": \"record\",\n" +
            "  \"namespace\": \"com.shared\",\n" +
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
            "}" +
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
                FileWriter amountWriter = new FileWriter(amountFile);
        ) {
            pizzaWriter.write(schema);
            amountWriter.write(dependency);
        }

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref = new Reference();
        ref.name = "com.shared.Amount";
        ref.subject = "Amount";
        subjectRefs.add(ref);
        schemaRefs.put("Pizza", subjectRefs);
        schemaRegistryMojo.references = schemaRefs;

        Map<String, File> schemas = new LinkedHashMap<>();
        schemas.put("Amount", amountFile);
        schemas.put("Pizza", pizzaFile);
        schemaRegistryMojo.subjects = schemas;

        schemaRegistryMojo.execute();
        Schema pizza = ((AvroSchema) schemaRegistryMojo.schemas.get("Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }

    @Test
    public void testSchemasWithSharedInlineDependencies() throws Exception {
        RegisterSchemaRegistryMojo schemaRegistryMojo = new RegisterSchemaRegistryMojo();
        schemaRegistryMojo.client = new MockSchemaRegistryClient();

        File pizzaFile = new File(tempDirectory, "pizza.avsc");
        File sodaFile = new File(tempDirectory, "soda.avsc");
        File comboFile = new File(tempDirectory, "combo.avsc");
        try (
                FileWriter pizzaWriter = new FileWriter(pizzaFile);
                FileWriter sodaWriter = new FileWriter(sodaFile);
                FileWriter comboWriter = new FileWriter(comboFile);
        ) {
            pizzaWriter.write(inlineSchema1);
            sodaWriter.write(inlineSchema2);
            comboWriter.write(combinedSchema);
        }

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref1 = new Reference();
        ref1.name = "com.pizza.Pizza";
        ref1.subject = "Pizza";
        subjectRefs.add(ref1);
        Reference ref2 = new Reference();
        ref2.name = "com.soda.Soda";
        ref2.subject = "Soda";
        subjectRefs.add(ref2);
        schemaRefs.put("Combo", subjectRefs);
        schemaRegistryMojo.references = schemaRefs;

        Map<String, File> schemas = new LinkedHashMap<>();
        schemas.put("Pizza", pizzaFile);
        schemas.put("Soda", sodaFile);
        schemas.put("Combo", comboFile);
        schemaRegistryMojo.subjects = schemas;

        schemaRegistryMojo.execute();

        Schema pizza = ((AvroSchema) schemaRegistryMojo.schemas.get("Pizza")).rawSchema();
        Schema soda = ((AvroSchema) schemaRegistryMojo.schemas.get("Soda")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertNotNull("The schema should've been generated", soda);
    }
}
