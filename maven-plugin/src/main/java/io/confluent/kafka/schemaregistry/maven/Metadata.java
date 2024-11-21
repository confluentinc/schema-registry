/*
 * Copyright 2023 Confluent Inc.
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.maven.plugins.annotations.Parameter;

public class Metadata {

  // For mojo, cannot have any constructors besides default constructor

  @Parameter(required = false)
  protected Map<String, Set<String>> tags = new HashMap<>();

  @Parameter(required = false)
  protected Map<String, String> properties = new HashMap<>();

  @Parameter(required = false)
  protected Set<String> sensitive = new HashSet<>();

  @Override
  public String toString() {
    return "Metadata{"
        + "tags=" + tags
        + ", properties=" + properties
        + ", sensitive=" + sensitive
        + '}';
  }

  public io.confluent.kafka.schemaregistry.client.rest.entities.Metadata toMetadataEntity() {
    return new io.confluent.kafka.schemaregistry.client.rest.entities.Metadata(
        tags, properties, sensitive
    );
  }
}
