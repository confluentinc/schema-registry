/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.resources;

import java.util.HashMap;
import java.util.Map;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;

import io.confluent.kafka.schemaregistry.client.rest.Versions;

@Path("/")
@Produces(
    {Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED, Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
     Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON, Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
           Versions.JSON, Versions.GENERIC_REQUEST})
public class RootResource {

  @GET
  public Map<String, String> get() {
    // Currently this just provides an endpoint that's a nop and can be used to check for
    // liveness and can be used for tests that need to test the server setup rather than the
    // functionality of a specific resource. Some APIs provide a listing of endpoints as their
    // root resource; it might be nice to provide that.
    return new HashMap<String, String>();
  }

  @POST
  public Map<String, String> post(@Valid Map<String, String> request) {
    // This version allows testing with posted entities
    return new HashMap<String, String>();
  }

}
