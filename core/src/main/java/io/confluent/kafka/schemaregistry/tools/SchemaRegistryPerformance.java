/*
 * Copyright 2015-2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.tools;

import io.confluent.common.utils.AbstractPerformanceTest;
import io.confluent.common.utils.PerformanceStats;
import io.confluent.kafka.schemaregistry.CompatibilityLevel;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

import java.io.IOException;
import java.util.Collections;

public class SchemaRegistryPerformance extends AbstractPerformanceTest {

  long targetRegisteredSchemas;
  long targetSchemasPerSec;
  String baseUrl;
  RestService restService;
  String subject;
  String schemaType;
  long registeredSchemas = 0;
  long successfullyRegisteredSchemas = 0;

  public static void main(String[] args) throws Exception {

    if (args.length < 4) {
      System.out.println(
          "Usage: java " + SchemaRegistryPerformance.class.getName() + " schema_registry_url"
          + " subject num_schemas target_schemas_per_sec schema_type"
      );
      System.exit(1);
    }

    String baseUrl = args[0];
    String subject = args[1];
    int numSchemas = Integer.parseInt(args[2]);
    int targetSchemasPerSec = Integer.parseInt(args[3]);
    String schemaType = args[4];

    SchemaRegistryPerformance perf =
        new SchemaRegistryPerformance(baseUrl, subject, numSchemas, targetSchemasPerSec,
            schemaType);
    perf.init();
    perf.run(targetSchemasPerSec);
    perf.close();
  }

  public SchemaRegistryPerformance(String baseUrl, String subject, long numSchemas,
                                   long targetSchemasPerSec, String schemaType) {
    super(numSchemas);
    this.baseUrl = baseUrl;
    this.restService = new RestService(baseUrl);
    this.subject = subject;
    this.schemaType = schemaType;
    this.targetRegisteredSchemas = numSchemas;
    this.targetSchemasPerSec = targetSchemasPerSec;
  }

  protected void init() throws Exception {
    // No compatibility verification
    ConfigUpdateRequest request = new ConfigUpdateRequest();
    request.setCompatibilityLevel(CompatibilityLevel.NONE.name);
    restService.updateConfig(request, null);
  }

  // sequential schema maker
  public static String makeSchema(String schemaType, long num) {
    String schemaString;
    switch (schemaType) {
      case AvroSchema.TYPE:
        schemaString = "{\"type\":\"record\","
            + "\"name\":\"myrecord\","
            + "\"fields\":"
            + "[{\"type\":\"string\",\"name\":"
            + "\"f" + num + "\"}]}";
        break;
      case JsonSchema.TYPE:
        schemaString = "{\"type\":\"object\",\"properties\":{\"f" + num + "\":{\"type"
            + "\":\"string\"}}}";
        break;
      case ProtobufSchema.TYPE:
        schemaString = "message Foo { required string f" + num + " = 1; }";
        break;
      default:
        throw new IllegalArgumentException("Unsupported schema type " + schemaType);
    }
    return schemaString;
  }

  // sequential schema maker
  public static ParsedSchema makeParsedSchema(String schemaType, long num) {
    String schema = makeSchema(schemaType, num);
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
    return parsedSchema;
  }

  @Override
  protected void doIteration(PerformanceStats.Callback cb) {
    String schema = makeSchema(this.schemaType, this.registeredSchemas);

    try {
      restService.registerSchema(schema, this.schemaType, Collections.emptyList(), this.subject);
      successfullyRegisteredSchemas++;
    } catch (IOException e) {
      System.out.println("Problem registering schema: " + e.getMessage());
    } catch (RestClientException e) {
      System.out.println("Problem registering schema: " + e.getMessage());
    }

    registeredSchemas++;
    cb.onCompletion(1, 0);
  }

  protected void close() throws IOException {
    // We can see some failures due to things like timeouts, but we want it to be obvious
    // if there are too many failures (indicating a real underlying problem). 1% is an arbitrarily
    // chosen limit.
    if (successfullyRegisteredSchemas / (double) targetRegisteredSchemas < 0.99) {
      throw new RuntimeException("Too many schema registration errors: "
                                 + successfullyRegisteredSchemas
                                 + " registered successfully out of " + targetRegisteredSchemas
                                 + " attempted");
    }
    if (restService != null) {
      restService.close();
    }
  }

  @Override
  protected boolean finished(int iteration) {
    return this.targetRegisteredSchemas == this.registeredSchemas;
  }

  @Override
  protected boolean runningFast(int iteration, float elapsed) {
    return iteration / elapsed > targetSchemasPerSec;
  }
}

