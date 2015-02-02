/*
 * Copyright 2015 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.exceptions;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/**
 * An exception thrown when the version is not a valid version id. Allowed values are between
 * [1, 2^31-1] and the string "latest"
 */
public class InvalidVersionException extends WebApplicationException {

  public static final Response.Status STATUS = Response.Status.BAD_REQUEST;

  public InvalidVersionException() {
    super("The specified version is not a valid version id. Allowed values are between "
          + "[1, 2^31-1] and the string \"latest\"", STATUS);
  }
}
