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
package io.confluent.kafka.schemaregistry.client.rest.exceptions;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * TODO: Fix as part of issue #66. This should extend RestConstraintViolationException or some 
 * subclass of RestException? Currently client does not depend on rest-utils.
 * * Indicates that the input schema is not a valid Avro schema.
 */
public class RestInvalidAvroException extends WebApplicationException {

  public static final Response.Status STATUS = Response.Status.BAD_REQUEST;

  public RestInvalidAvroException() {
    super("The provided schema string is not a valid Avro schema", STATUS);
  }

  public RestInvalidAvroException(String message) {
    super(message, STATUS);
  }

  public RestInvalidAvroException(String message, Throwable cause) {
    super(message, STATUS);
  }
}
