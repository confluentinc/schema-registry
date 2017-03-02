/**
 * Copyright 2016 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public abstract class SchemaRegistryMojo extends AbstractMojo {

  @Parameter(required = true)
  List<String> schemaRegistryUrls;
  private SchemaRegistryClient client;

  void client(SchemaRegistryClient client) {
    this.client = client;
  }

  protected SchemaRegistryClient client() {
    if (null == this.client) {
      this.client = new CachedSchemaRegistryClient(this.schemaRegistryUrls, 1000);
    }

    return this.client;
  }

  protected Map<String, Schema> loadSchemas(Map<String, File> subjects) {
    int errorCount = 0;
    Map<String, Schema> results = new LinkedHashMap<>();

    for (Map.Entry<String, File> kvp : subjects.entrySet()) {
      Schema.Parser parser = new Schema.Parser();
      getLog().debug(
          String.format(
              "Loading schema for subject(%s) from %s.",
              kvp.getKey(),
              kvp.getValue()
          )
      );

      try (FileInputStream inputStream = new FileInputStream(kvp.getValue())) {
        Schema schema = parser.parse(inputStream);
        results.put(kvp.getKey(), schema);
      } catch (IOException ex) {
        getLog().error("Exception thrown while loading " + kvp.getValue(), ex);
        errorCount++;
      } catch (SchemaParseException ex) {
        getLog().error("Exception thrown while parsing " + kvp.getValue(), ex);
        errorCount++;
      }
    }

    if (errorCount > 0) {
      throw new IllegalStateException("One or more schemas could not be loaded.");
    }

    return results;
  }
}
