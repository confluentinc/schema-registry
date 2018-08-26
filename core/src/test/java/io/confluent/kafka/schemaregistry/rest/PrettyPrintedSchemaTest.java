/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
public class PrettyPrintedSchemaTest extends ClusterTestHarness {

    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {SchemaRegistryConfig.RESOURCE_EXTENSION_CONFIG},
                {SchemaRegistryConfig.SCHEMAREGISTRY_RESOURCE_EXTENSION_CONFIG}
        });
    }

    @Parameter
    public String resourceExtensionConfigName;

    public PrettyPrintedSchemaTest() {
        super(1, true, AvroCompatibilityLevel.BACKWARD.name);
    }

    String prettySchemaString1 = "{\n"+
            "  \"type\" : \"record\",\n"+
            "  \"name\" : \"myrecord\",\n"+
            "  \"fields\" : [ {\n"+
            "    \"name\" : \"f1\",\n"+
            "    \"type\" : \"string\"\n"+
            "  } ]\n"+
            "}";

    String schemaString1 = AvroUtils.parseSchema(prettySchemaString1).canonicalString;

    private void comparePrettySchemas(String prettySchema, String s) {
        assertNotEquals(
                String.format("%s: Record schema should not be the same after prettified", s),
                schemaString1,
                prettySchema);
        assertEquals(
                String.format("%s: Record schema should be prettified", s),
                prettySchemaString1,
                prettySchema);
    }

    @Test
    public void testPrettyVersion() throws Exception {
        String subject = "testSubject";

        int expectedIdSchema1 = 1;
        assertEquals(
                "Registering should succeed",
                expectedIdSchema1,
                restApp.restClient.registerSchema(schemaString1, subject)
        );

        String prettySchema = restApp.restClient.getVersionSchemaOnly(subject, expectedIdSchema1, false);
        assertEquals(
                "Record schema should be unaffected",
                schemaString1,
                prettySchema);

        prettySchema = restApp.restClient.getVersionSchemaOnly(subject, expectedIdSchema1, true);
        comparePrettySchemas(prettySchema, "v"+expectedIdSchema1);

        prettySchema = restApp.restClient.getLatestVersionSchemaOnly(subject, true);
        comparePrettySchemas(prettySchema, "latest");
    }

    @Test
    public void testPrettyIdSchema() throws Exception {
        String subject = "testSubject";

        int expectedIdSchema1 = 1;
        assertEquals(
                "Registering should succeed",
                expectedIdSchema1,
                restApp.restClient.registerSchema(schemaString1, subject)
        );

        String prettySchema = restApp.restClient.getIdSchemaOnly(expectedIdSchema1, false);
        assertEquals(
                "Record schema should not be prettified",
                schemaString1,
                prettySchema);

        prettySchema = restApp.restClient.getIdSchemaOnly(expectedIdSchema1, true);
        comparePrettySchemas(prettySchema, "v"+expectedIdSchema1);
    }

}
