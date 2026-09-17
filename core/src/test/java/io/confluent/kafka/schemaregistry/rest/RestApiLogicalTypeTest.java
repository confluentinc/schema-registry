/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.RestApp;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Collections;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Registers schemas written as logical types DDL and reads them back, over a live registry.
 */
@Tag("IntegrationTest")
public abstract class RestApiLogicalTypeTest {

  private static final String LOGICAL = "logical";

  protected RestApp restApp = null;

  public void setRestApp(RestApp restApp) {
    this.restApp = restApp;
  }

  /**
   * A union's branch names survive only in metadata. Avro has no way to name a union member, so
   * {@code ["string","int"]} alone cannot say which branch was {@code mystr} and which was
   * {@code myint} -- the conversion records that under {@code confluent:union}, and reading the
   * schema back as DDL reconstructs the branches from it. Losing that metadata on the way into
   * storage would leave a union that still registers and still reads, but comes back naming its
   * branches something else.
   */
  @Test
  public void testAvroUnionRoundTripsThroughLogicalFormat() throws Exception {
    String subject = "union-value";
    String ddl = "TYPE UNION(mystr STRING, myint INT)";

    restApp.restClient.registerSchema(ddl, AvroSchema.TYPE, Collections.emptyList(), subject);

    // Stored natively: the registry converts the DDL, so what it holds is Avro, not the script.
    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertEquals(AvroSchema.TYPE, stored.getSchemaType());
    assertTrue(stored.getSchema().contains("\"string\""), stored.getSchema());
    assertTrue(stored.getSchema().contains("\"int\""), stored.getSchema());

    assertNotNull(stored.getMetadata(), "the conversion's metadata must be stored");
    assertTrue(stored.getMetadata().getProperties().containsKey("confluent:union"),
        "branch identity lives here: " + stored.getMetadata().getProperties());

    // Read back as DDL: the branch names come from the metadata, not from the Avro text.
    Schema logical = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1, LOGICAL, false, null);
    assertTrue(logical.getSchema().contains("UNION"), logical.getSchema());
    assertTrue(logical.getSchema().contains("mystr"), logical.getSchema());
    assertTrue(logical.getSchema().contains("myint"), logical.getSchema());
  }

  /**
   * A DDL body is recognized by its own syntax, so a native schema submitted under the same
   * schemaType must still be stored as it was written.
   */
  @Test
  public void testAvroSchemaIsNotTreatedAsLogical() throws Exception {
    String subject = "native-value";
    String avro = "{\"type\":\"record\",\"name\":\"Widget\",\"fields\":"
        + "[{\"name\":\"id\",\"type\":\"string\"}]}";

    restApp.restClient.registerSchema(avro, AvroSchema.TYPE, Collections.emptyList(), subject);

    Schema stored = restApp.restClient.getVersion(
        RestService.DEFAULT_REQUEST_PROPERTIES, subject, 1);
    assertEquals(AvroSchema.TYPE, stored.getSchemaType());
    assertTrue(stored.getSchema().contains("Widget"), stored.getSchema());
  }
}
