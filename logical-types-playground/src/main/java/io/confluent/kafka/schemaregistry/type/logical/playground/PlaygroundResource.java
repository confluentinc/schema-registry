/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.playground;

import io.confluent.kafka.schemaregistry.type.logical.playground.dto.CompleteRequest;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.CompleteResponse;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.ConvertRequest;
import io.confluent.kafka.schemaregistry.type.logical.playground.dto.ConvertResponse;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/api")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PlaygroundResource {

  @Inject
  CompletionService completionService;

  @Inject
  ConversionService conversionService;

  @POST
  @Path("/complete")
  public CompleteResponse complete(CompleteRequest req) {
    return completionService.complete(
        req.sql == null ? "" : req.sql,
        req.caretOffset);
  }

  @POST
  @Path("/convert")
  public ConvertResponse convert(ConvertRequest req) {
    return conversionService.convert(req);
  }
}
