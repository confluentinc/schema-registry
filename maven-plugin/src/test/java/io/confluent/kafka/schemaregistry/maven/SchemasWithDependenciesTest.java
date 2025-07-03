/*
 * Copyright 2019-2021 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
