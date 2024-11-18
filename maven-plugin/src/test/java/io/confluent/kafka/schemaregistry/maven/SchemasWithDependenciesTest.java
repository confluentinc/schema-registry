package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;
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

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref = new Reference();
        ref.name = "com.pizza.Amount";
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
    public void testSchemaWithDependenciesAndMetadata() throws Exception {
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

        Map<String, Set<String>> tags = new HashMap<>();
        tags.put("**.ssn", Collections.singleton("PII"));
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        Set<String> sensitive = Collections.singleton("key1");
        Metadata m = new Metadata();
        m.tags = tags;
        m.properties = properties;
        m.sensitive = sensitive;
        schemaRegistryMojo.metadata = Collections.singletonMap("Pizza", m);

        Rule rule = new Rule();
        rule.name = "rule1";
        rule.kind = RuleKind.TRANSFORM;
        rule.mode = RuleMode.WRITEREAD;
        rule.type = "ENCRYPT";
        rule.tags = Collections.singleton("PII");
        List<Rule> domainRules = Collections.singletonList(rule);
        RuleSet rs = new RuleSet();
        rs.migrationRules = Collections.emptyList();
        rs.domainRules = domainRules;
        schemaRegistryMojo.ruleSet = Collections.singletonMap("Pizza", rs);

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref = new Reference();
        ref.name = "com.pizza.Amount";
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
        io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata =
            schemaRegistryMojo.schemas.get("Pizza").metadata();
        io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet =
            schemaRegistryMojo.schemas.get("Pizza").ruleSet();
        Assert.assertEquals(m.toMetadataEntity(), metadata);
        Assert.assertEquals(rs.toRuleSetEntity(), ruleSet);

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }

    @Test
    public void testSchemaWithPreregisteredDependencies() throws Exception {
        RegisterSchemaRegistryMojo schemaRegistryMojo = new RegisterSchemaRegistryMojo();
        schemaRegistryMojo.client = new MockSchemaRegistryClient();

        File pizzaFile = new File(tempDirectory, "pizza.avsc");
        try (
            FileWriter pizzaWriter = new FileWriter(pizzaFile);
        ) {
            pizzaWriter.write(schema);
        }

        // Preregister Amount
        schemaRegistryMojo.client.register("Amount", new AvroSchema(dependency));

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref = new Reference();
        ref.name = "com.pizza.Amount";
        ref.subject = "Amount";
        subjectRefs.add(ref);
        schemaRefs.put("Pizza", subjectRefs);
        schemaRegistryMojo.references = schemaRefs;

        Map<String, File> schemas = new LinkedHashMap<>();
        // Don't include Amount
        schemas.put("Pizza", pizzaFile);
        schemaRegistryMojo.subjects = schemas;

        schemaRegistryMojo.execute();
        Schema pizza = ((AvroSchema) schemaRegistryMojo.schemas.get("Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }

    @Test
    public void testSchemaWithDependenciesWithSlash() throws Exception {
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

        Map<String, List<Reference>> schemaRefs = new LinkedHashMap<>();
        List<Reference> subjectRefs = new ArrayList<>();
        Reference ref = new Reference();
        ref.name = "com.pizza.Amount";
        ref.subject = "Root/Amount";
        subjectRefs.add(ref);
        schemaRefs.put("Root_x2FPizza", subjectRefs);
        schemaRegistryMojo.references = schemaRefs;

        Map<String, File> schemas = new LinkedHashMap<>();
        schemas.put("Root_x2FAmount", amountFile);
        schemas.put("Root_x2FPizza", pizzaFile);
        schemaRegistryMojo.subjects = schemas;

        schemaRegistryMojo.execute();
        Schema pizza = ((AvroSchema) schemaRegistryMojo.schemas.get("Root/Pizza")).rawSchema();

        Assert.assertNotNull("The schema should've been generated", pizza);
        Assert.assertTrue("The schema should contain fields from the dependency", pizza.toString().contains("currency"));
    }

}
