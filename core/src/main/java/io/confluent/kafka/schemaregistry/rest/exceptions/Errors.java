/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.exceptions;

import javax.ws.rs.core.Response;

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class Errors {

  // HTTP 404
  public static final String SUBJECT_NOT_FOUND_MESSAGE_FORMAT = "Subject '%s' not found.";
  public static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;
  public static final String VERSION_NOT_FOUND_MESSAGE_FORMAT = "Version %s not found.";
  public static final int VERSION_NOT_FOUND_ERROR_CODE = 40402;
  public static final String SCHEMA_NOT_FOUND_MESSAGE = "Schema not found";
  public static final String SCHEMA_NOT_FOUND_MESSAGE_FORMAT = "Schema %s not found";
  public static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;

  // HTTP 409
  public static final int INCOMPATIBLE_SCHEMA_ERROR_CODE = Response.Status.CONFLICT.getStatusCode();

  // HTTP 422
  public static final int INVALID_SCHEMA_ERROR_CODE = 42201;
  public static final int INVALID_VERSION_ERROR_CODE = 42202;
  public static final int INVALID_COMPATIBILITY_LEVEL_ERROR_CODE = 42203;
  public static final int INVALID_MODE_ERROR_CODE = 42204;
  public static final int OPERATION_NOT_PERMITTED_ERROR_CODE = 42205;

  // HTTP 500
  public static final int STORE_ERROR_CODE = 50001;
  public static final int OPERATION_TIMEOUT_ERROR_CODE = 50002;
  public static final int REQUEST_FORWARDING_FAILED_ERROR_CODE = 50003;
  public static final int UNKNOWN_MASTER_ERROR_CODE = 50004;
  // 50005 is used by the RestService to indicate a JSON Parse Error

  public static RestException subjectNotFoundException(String subject) {
    return new RestNotFoundException(
        String.format(SUBJECT_NOT_FOUND_MESSAGE_FORMAT, subject), SUBJECT_NOT_FOUND_ERROR_CODE);
  }

  public static RestException versionNotFoundException(Integer id) {
    return new RestNotFoundException(
        String.format(VERSION_NOT_FOUND_MESSAGE_FORMAT, id), VERSION_NOT_FOUND_ERROR_CODE);
  }

  public static RestException schemaNotFoundException() {
    return new RestNotFoundException(SCHEMA_NOT_FOUND_MESSAGE, SCHEMA_NOT_FOUND_ERROR_CODE);
  }

  public static RestException schemaNotFoundException(Integer id) {
    if (id == null) {
      return schemaNotFoundException();
    }
    return new RestNotFoundException(
        String.format(SCHEMA_NOT_FOUND_MESSAGE_FORMAT, id), SCHEMA_NOT_FOUND_ERROR_CODE);
  }

  public static RestIncompatibleAvroSchemaException incompatibleSchemaException(String message,
                                                                                Throwable cause) {
    return new RestIncompatibleAvroSchemaException(message,
                                                   RestIncompatibleAvroSchemaException.DEFAULT_ERROR_CODE,
                                                   cause);
  }

  public static RestInvalidSchemaException invalidAvroException(String message, Throwable cause) {
    return new RestInvalidSchemaException(message);
  }

  public static RestInvalidVersionException invalidVersionException(String version) {
    return new RestInvalidVersionException(version);
  }

  public static RestException schemaRegistryException(String message, Throwable cause) {
    return new RestSchemaRegistryException(message, cause);
  }

  public static RestException storeException(String message, Throwable cause) {
    return new RestSchemaRegistryStoreException(message, cause);
  }

  public static RestException operationTimeoutException(String message, Throwable cause) {
    return new RestSchemaRegistryTimeoutException(message, cause);
  }

  public static RestOperationNotPermittedException operationNotPermittedException(String message) {
    return new RestOperationNotPermittedException(message);
  }

  public static RestException requestForwardingFailedException(String message, Throwable cause) {
    return new RestRequestForwardingException(message, cause);
  }

  public static RestException unknownMasterException(String message, Throwable cause) {
    return new RestUnknownMasterException(message, cause);
  }
}
