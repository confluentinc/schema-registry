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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestInvalidAvroException;
import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class Errors {

  // HTTP 404
  public final static String SUBJECT_NOT_FOUND_MESSAGE = "Subject not found.";
  public final static int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;
  public final static String VERSION_NOT_FOUND_MESSAGE = "Version not found.";
  public final static int VERSION_NOT_FOUND_ERROR_CODE = 40402;
  public final static String SCHEMA_NOT_FOUND_MESSAGE = "Schema not found";
  public final static int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;

  // HTTP 422
  public static final int INVALID_SCHEMA_ERROR_CODE = 42201;
  public static final int INVALID_VERSION_ERROR_CODE = 42202;
  public static final int INVALID_COMPATIBILITY_LEVEL_ERROR_CODE = 42203;

  // HTTP 500
  public static final int STORE_ERROR_CODE = 50001;
  public static final int OPERATION_TIMEOUT_ERROR_CODE = 50002;
  public static final int REQUEST_FORWARDING_FAILED_ERROR_CODE = 50003;
  public static final int UNKNOWN_MASTER_ERROR_CODE = 50004;

  public static RestException subjectNotFoundException() {
    return new RestNotFoundException(SUBJECT_NOT_FOUND_MESSAGE, SUBJECT_NOT_FOUND_ERROR_CODE);
  }

  public static RestException versionNotFoundException() {
    return new RestNotFoundException(VERSION_NOT_FOUND_MESSAGE, VERSION_NOT_FOUND_ERROR_CODE);
  }

  public static RestException schemaNotFoundException() {
    return new RestNotFoundException(SCHEMA_NOT_FOUND_MESSAGE, SCHEMA_NOT_FOUND_ERROR_CODE);
  }
  
  public static RestInvalidAvroException invalidAvroException(String message, Throwable cause) {
    return new RestInvalidAvroException(message, cause);
  }
  
  public static RestInvalidVersionException invalidVersionException() {
    return new RestInvalidVersionException();
  }
  
  public static RestException schemaRegistryException(String message, Throwable cause) {
    throw new RestSchemaRegistryException(message, cause);    
  }
  
  public static RestException storeException(String message, Throwable cause) {
    throw new RestSchemaRegistryStoreException(message, cause);    
  }

  public static RestException operationTimeoutException(String message, Throwable cause) {
    throw new RestSchemaRegistryTimeoutException(message, cause);
  }

  public static RestException requestForwardingFailedException(String message, Throwable cause) {
    throw new RestRequestForwardingException(message, cause);
  }

  public static RestException unknownMasterException(String message, Throwable cause) {
    throw new RestUnknownMasterException(message, cause);
  }
}
