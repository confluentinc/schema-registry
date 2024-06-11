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

package io.confluent.kafka.schemaregistry.maven;

import org.apache.kafka.common.utils.Utils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;

public abstract class SchemaRegistryMojo extends AbstractMojo implements Closeable {

  @Parameter(required = true)
  List<String> schemaRegistryUrls;

  @Parameter
  String userInfoConfig;

  @Parameter(property = "kafka-schema-registry.skip")
  boolean skip;

  @Parameter(required = false)
  List<String> schemaProviders = new ArrayList<>();

  @Parameter
  Map<String, String> configs = new HashMap<>();

  protected SchemaRegistryClient client;

  void client(SchemaRegistryClient client) {
    this.client = client;
  }

  protected SchemaRegistryClient client() {
    if (null == this.client) {
      Map<String, String> config = new HashMap<>();
      if (configs != null && !configs.isEmpty()) {
        config.putAll(configs);
      }
      if (userInfoConfig != null) {
        // Note that BASIC_AUTH_CREDENTIALS_SOURCE is not configurable as the plugin only supports
        // a single schema registry URL, so there is no additional utility of the URL source.
        config.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        config.put(SchemaRegistryClientConfig.USER_INFO_CONFIG, userInfoConfig);
      }
      List<SchemaProvider> providers = schemaProviders != null && !schemaProviders.isEmpty()
                                       ? schemaProviders()
                                       : defaultSchemaProviders();
      this.client = new CachedSchemaRegistryClient(
          this.schemaRegistryUrls,
          1000,
          providers,
          config
      );
    }
    return this.client;
  }

  private List<SchemaProvider> schemaProviders() {
    return schemaProviders.stream().map(s -> {
      try {
        return Utils.newInstance(s, SchemaProvider.class);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  private List<SchemaProvider> defaultSchemaProviders() {
    return Arrays.asList(
        new AvroSchemaProvider(), new JsonSchemaProvider(), new ProtobufSchemaProvider()
    );
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }
}
