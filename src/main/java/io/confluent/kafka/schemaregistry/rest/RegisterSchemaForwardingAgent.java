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
package io.confluent.kafka.schemaregistry.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import io.confluent.kafka.schemaregistry.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.utils.RestUtils;

/**
 * An agent responsible for forwarding an incoming registering schema request to another HTTP
 * server
 */
public class RegisterSchemaForwardingAgent {

  private static final Logger log = LoggerFactory.getLogger(RegisterSchemaForwardingAgent.class);
  private final Map<String, String> requestProperties;
  private final String subject;
  private final RegisterSchemaRequest registerSchemaRequest;

  public RegisterSchemaForwardingAgent(Map<String, String> requestProperties, String subject,
                                       RegisterSchemaRequest registerSchemaRequest) {
    this.requestProperties = requestProperties;
    this.subject = subject;
    this.registerSchemaRequest = registerSchemaRequest;
  }

  /**
   * Forward the request
   *
   * @param host host to forward the request to
   * @param port port to forward the request to
   * @return The id of the schema if registration is successful. Otherwise, throw a
   * WebApplicationException.
   */
  public int forward(String host, int port) throws SchemaRegistryException {
    String baseUrl = String.format("http://%s:%d", host, port);
    log.debug(String.format("Forwarding registering schema request %s to %s",
                            registerSchemaRequest, baseUrl));
    try {
      int id = RestUtils.registerSchema(baseUrl, requestProperties, registerSchemaRequest,
                                         subject);
      return id;
    } catch (IOException e) {
      throw new SchemaRegistryException(
          String.format("Unexpected error while forwarding the registering schema request %s to %s",
                        registerSchemaRequest, baseUrl),
          e);
    }
  }
}
