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

package io.confluent.kafka.schemaregistry.client.config.provider;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link ConfigProvider} that obtains configs from metadata in schema
 * registry.
 */
public class SchemaRegistryConfigProvider implements ConfigProvider {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryConfigProvider.class);

  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  private SchemaRegistryClient schemaRegistry;

  public void configure(Map<String, ?> configs) {
    String urlString = (String) configs.get("schema.registry.url");
    if (urlString == null) {
      throw new IllegalArgumentException("Missing schema.registry.url");
    }
    List<String> urls = Arrays.asList(urlString.split(","));

    if (schemaRegistry == null) {
      schemaRegistry = SchemaRegistryClientFactory.newClient(
          urls,
          DEFAULT_CACHE_CAPACITY,
          Collections.emptyList(),
          configs,
          Collections.emptyMap()
      );
    }
  }

  /**
   * Retrieves the data from schema registry at the given path. The path is of the form
   * contexts/{context}/subjects/{subject}/versions/{version} or
   * subjects/{subject}/versions/{version} or
   * subjects/{subject} (same as using a version of -1 or latest).
   *
   * @param path the path where the data resides
   * @return the configuration data
   */
  public ConfigData get(String path) {
    return new ConfigData(getData(path));
  }

  /**
   * Retrieves the data with the given keys at the given path. The path is of the form
   * contexts/{context}/subjects/{subject}/versions/{version} or
   * subjects/{subject}/versions/{version} or
   * subjects/{subject} (same as using a version of -1 or latest).
   *
   * @param path the path where the data resides
   * @param keys the keys whose values will be retrieved
   * @return the configuration data
   */
  public ConfigData get(String path, Set<String> keys) {
    Map<String, String> data = getData(path).entrySet().stream()
        .filter(s -> keys.contains(s.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    return new ConfigData(data);
  }

  public Map<String, String> getData(String path) {
    try {
      if (path == null) {
        return Collections.emptyMap();
      }
      String[] parts = getSubjectAndVersion(path);
      String subject = parts[0];
      if (subject == null) {
        return Collections.emptyMap();
      }
      int version;
      String versionStr = parts[1];
      if ("latest".equals(versionStr)) {
        version = -1;
      } else {
        try {
          version = Integer.parseInt(versionStr);
        } catch (NumberFormatException e) {
          return Collections.emptyMap();
        }
      }
      SchemaMetadata schema = version >= 0
          ? schemaRegistry.getSchemaMetadata(subject, version, true)
          : schemaRegistry.getLatestSchemaMetadata(subject);
      Metadata metadata = schema.getMetadata();
      if (metadata == null) {
        return Collections.emptyMap();
      }
      Map<String, String> props = metadata.getProperties();
      return props != null ? props : Collections.emptyMap();
    } catch (IOException | RestClientException e) {
      log.error("Could not obtain config data", e);
      throw new RuntimeException(e);
    }
  }

  private String[] getSubjectAndVersion(String path) {
    String context = null;
    String subject = null;
    String version = "-1";
    boolean contextPathFound = false;
    boolean subjectPathFound = false;
    boolean versionPathFound = false;
    for (String uriPathStr : path.split("/")) {
      if (contextPathFound) {
        context = uriPathStr;
        contextPathFound = false;
      } else if (uriPathStr.equals("contexts")) {
        contextPathFound = true;
      } else if (subjectPathFound) {
        subject = uriPathStr;
        subjectPathFound = false;
      } else if (uriPathStr.equals("subjects")) {
        subjectPathFound = true;
      } else if (versionPathFound) {
        version = uriPathStr;
        versionPathFound = false;
      } else if (uriPathStr.equals("versions")) {
        versionPathFound = true;
      }
    }

    String delimitedContext = context != null
        ? CONTEXT_DELIMITER + context + CONTEXT_DELIMITER
        : "";

    return new String[]{subject != null ? delimitedContext + subject : null, version};
  }

  @Override
  public void close() throws IOException {
    schemaRegistry.reset();
    schemaRegistry.close();
  }
}
