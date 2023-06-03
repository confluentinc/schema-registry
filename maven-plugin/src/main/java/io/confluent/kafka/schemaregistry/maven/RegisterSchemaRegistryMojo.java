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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.File;
import org.apache.maven.plugins.annotations.Mojo;

import java.io.IOException;
import java.util.Map;
import org.apache.maven.plugins.annotations.Parameter;

@Mojo(name = "register", configurator = "custom-basic")
public class RegisterSchemaRegistryMojo extends UploadSchemaRegistryMojo {

  @Parameter(required = false)
  boolean normalizeSchemas = false;

  @Override
  protected boolean processSchema(String subject,
                                  File schemaPath,
                                  ParsedSchema schema,
                                  Map<String, Integer> schemaVersions)
      throws IOException, RestClientException {

    if (getLog().isDebugEnabled()) {
      getLog().debug(
          String.format("Calling register('%s', '%s')", subject, schema)
      );
    }

    Integer id = this.client().register(subject, schema, normalizeSchemas);
    Integer version = this.client().getVersion(subject, schema, normalizeSchemas);
    getLog().info(
        String.format(
            "Registered subject(%s) with id %s version %s",
            subject,
            id,
            version
        ));
    schemaVersions.put(subject, version);
    return true;
  }
}
