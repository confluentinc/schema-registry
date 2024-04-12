/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.handlers;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.ConfigUpdateRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.TagSchemaRequest;
import java.util.List;

public class CompositeUpdateRequestHandler implements UpdateRequestHandler {

  private List<UpdateRequestHandler> handlers;

  public CompositeUpdateRequestHandler(List<UpdateRequestHandler> handlers) {
    this.handlers = handlers;
  }

  @Override
  public void handle(ConfigUpdateRequest request) {
    for (UpdateRequestHandler handler : handlers) {
      handler.handle(request);
    }
  }

  @Override
  public void handle(String subject, ConfigUpdateRequest request) {
    for (UpdateRequestHandler handler : handlers) {
      handler.handle(subject, request);
    }
  }

  @Override
  public void handle(String subject, boolean normalize, RegisterSchemaRequest request) {
    for (UpdateRequestHandler handler : handlers) {
      handler.handle(subject, normalize, request);
    }
  }

  @Override
  public void handle(Schema schema, TagSchemaRequest request) {
    for (UpdateRequestHandler handler : handlers) {
      handler.handle(schema, request);
    }
  }
}
