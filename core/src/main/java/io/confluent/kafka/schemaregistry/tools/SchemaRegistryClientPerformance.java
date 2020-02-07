/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.tools;

import java.io.IOException;

import io.confluent.common.utils.PerformanceStats;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

public class SchemaRegistryClientPerformance extends SchemaRegistryPerformance {

  CachedSchemaRegistryClient client;

  public static void main(String[] args) throws Exception {

    if (args.length < 4) {
      System.out.println(
          "Usage: java " + SchemaRegistryClientPerformance.class.getName() + " schema_registry_url"
          + " subject num_schemas target_schemas_per_sec schema_type"
      );
      System.exit(1);
    }

    String baseUrl = args[0];
    String subject = args[1];
    int numSchemas = Integer.parseInt(args[2]);
    int targetSchemasPerSec = Integer.parseInt(args[3]);
    String schemaType = args[4];

    SchemaRegistryClientPerformance perf =
        new SchemaRegistryClientPerformance(baseUrl, subject, numSchemas, targetSchemasPerSec,
            schemaType);
    perf.init();
    perf.run(targetSchemasPerSec);
    perf.close();
  }

  public SchemaRegistryClientPerformance(String baseUrl, String subject, long numSchemas,
                                         long targetSchemasPerSec,
                                         String schemaType) {
    super(baseUrl, subject, numSchemas, targetSchemasPerSec, schemaType);

    client = new CachedSchemaRegistryClient(restService, Integer.MAX_VALUE);
  }

  @Override
  protected void init() throws Exception {
    // No compatibility verification
    client.updateCompatibility(null, CompatibilityLevel.NONE.name);
  }

  @Override
  protected void doIteration(PerformanceStats.Callback cb) {
    String schema = makeSchema(this.schemaType, this.registeredSchemas);
    ParsedSchema parsedSchema;
    switch (schemaType) {
      case AvroSchema.TYPE:
        parsedSchema = new AvroSchema(schema);
        break;
      case JsonSchema.TYPE:
        parsedSchema = new JsonSchema(schema);
        break;
      case ProtobufSchema.TYPE:
        parsedSchema = new ProtobufSchema(schema);
        break;
      default:
        throw new IllegalArgumentException("Unsupported schema type " + schemaType);
    }

    try {
      client.register(this.subject, parsedSchema);
      client.getId(this.subject, parsedSchema);
      client.getVersion(this.subject, parsedSchema);
      successfullyRegisteredSchemas++;
    } catch (IOException e) {
      System.out.println("Problem registering schema: " + e.getMessage());
    } catch (RestClientException e) {
      System.out.println("Problem registering schema: " + e.getMessage());
    }

    registeredSchemas++;
    cb.onCompletion(1, 0);
  }
}

