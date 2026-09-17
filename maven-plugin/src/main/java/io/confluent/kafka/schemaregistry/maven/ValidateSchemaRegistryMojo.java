/*
 * Copyright 2020 Confluent Inc.
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
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import org.apache.maven.plugins.annotations.Mojo;

@Mojo(name = "validate", configurator = "custom-basic")
public class ValidateSchemaRegistryMojo extends UploadSchemaRegistryMojo {

  @Override
  protected boolean processSchema(String subject,
                                  File schemaPath,
                                  RegisterSchemaRequest request,
                                  Map<String, Integer> schemaVersions)
      throws IOException, RestClientException {

    if (getLog().isDebugEnabled()) {
      getLog().debug(
          String.format("Calling validate('%s', '%s')", subject, request.getSchema())
      );
    }
    Optional<ParsedSchema> schema = this.client().parseSchema(
        request.getSchemaType(), request.getSchema(), request.getReferences(),
        request.getMetadata(), request.getRuleSet());
    if (schema.isPresent()) {
      schema.get().validate(false);
    } else {
      LogicalSchema.validate(subject, request.getSchemaType(), request.getSchema(), false);
    }
    return true;
  }
}
