/*
 * Copyright 2022 Confluent Inc.
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveAvroSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveJsonSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveProtobufSchema;
import io.confluent.kafka.schemaregistry.maven.derive.schema.DeriveSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.FileOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Mojo(name = "derive-schema", configurator = "custom-basic")
public class DeriveSchemaMojo extends AbstractMojo {

  @Parameter(required = true)
  File messagePath;

  @Parameter(required = true)
  File outputPath;

  @Parameter(defaultValue = "AVRO")
  String schemaType;

  protected static final ObjectMapper mapper = JacksonMapper.INSTANCE;

  public static void writeOutput(File outputPath, JsonNode schemaInformation) throws IOException {
    FileOutputStream fileStream = new FileOutputStream(outputPath.getPath());
    OutputStreamWriter writer = new OutputStreamWriter(fileStream,
        StandardCharsets.UTF_8);
    writer.write(schemaInformation.toPrettyString());
    writer.close();
  }

  public static List<JsonNode> readLinesOfMessages(File messagePath) throws IOException {
    String content = MojoUtils.readFile(messagePath, StandardCharsets.UTF_8);
    List<JsonNode> listOfMessages = new ArrayList<>();
    String[] lineSeparatedMessages = content.split("\n");
    for (int i = 0; i < lineSeparatedMessages.length; i++) {
      // Ignore empty messages
      if (lineSeparatedMessages[i].length() == 0) {
        continue;
      }
      try {
        listOfMessages.add(mapper.readValue(lineSeparatedMessages[i], ObjectNode.class));
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format("Message on line %d is an invalid JSON message", i + 1), e);
      }
    }
    return listOfMessages;
  }

  @Override
  public void execute() throws MojoExecutionException {
    List<JsonNode> listOfMessages;
    try {
      listOfMessages = readLinesOfMessages(messagePath);
    } catch (IOException e) {
      throw new MojoExecutionException("Exception thrown while reading input file", e);
    }

    DeriveSchema derive;
    if (schemaType.equalsIgnoreCase(JsonSchema.TYPE)) {
      derive = new DeriveJsonSchema();
    } else if (schemaType.equalsIgnoreCase(ProtobufSchema.TYPE)) {
      derive = new DeriveProtobufSchema();
    } else if (schemaType.equalsIgnoreCase(AvroSchema.TYPE)) {
      derive = new DeriveAvroSchema();
    } else {
      throw new MojoExecutionException("Schema type should be one of avro, json or protobuf");
    }

    JsonNode schemaInformation;
    try {
      schemaInformation = derive.getSchemaForMultipleMessages(listOfMessages);
    } catch (Exception e) {
      throw new MojoExecutionException("Exception thrown while deriving schema", e);
    }

    try {
      writeOutput(outputPath, schemaInformation);
    } catch (IOException e) {
      throw new MojoExecutionException("Exception thrown while writing to output file", e);
    }
  }
}