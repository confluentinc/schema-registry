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
  public static final String SUBJECT_SOFT_DELETED_MESSAGE_FORMAT = "Subject '%s' was soft deleted."
          + "Set permanent=true to delete permanently";
  public static final int SUBJECT_SOFT_DELETED_ERROR_CODE = 40404;
  public static final String SUBJECT_NOT_SOFT_DELETED_MESSAGE_FORMAT = "Subject '%s' was not deleted "
          + "first before being permanently deleted";
  public static final int SUBJECT_NOT_SOFT_DELETED_ERROR_CODE = 40405;
  public static final String SCHEMAVERSION_SOFT_DELETED_MESSAGE_FORMAT = "Subject '%s' Version %s was soft deleted."
          + "Set permanent=true to delete permanently";
  public static final int SCHEMAVERSION_SOFT_DELETED_ERROR_CODE = 40406;
  public static final String SCHEMAVERSION_NOT_SOFT_DELETED_MESSAGE_FORMAT = "Subject '%s' Version %s was not deleted "
          + "first before being permanently deleted";
  public static final int SCHEMAVERSION_NOT_SOFT_DELETED_ERROR_CODE = 40407;
  public static final String SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_MESSAGE_FORMAT = "Subject '%s' does not have "
          + "subject-level compatibility configured";
  public static final int SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE = 40408;
  public static final String SUBJECT_LEVEL_MODE_NOT_CONFIGURED_MESSAGE_FORMAT = "Subject '%s' does not have "
          + "subject-level mode configured";
  public static final int SUBJECT_LEVEL_MODE_NOT_CONFIGURED_ERROR_CODE = 40409;



  // HTTP 409
  public static final int INCOMPATIBLE_SCHEMA_ERROR_CODE = Response.Status.CONFLICT.getStatusCode();

  // HTTP 422
  public static final int INVALID_SCHEMA_ERROR_CODE = 42201;
  public static final int INVALID_VERSION_ERROR_CODE = 42202;
  public static final int INVALID_COMPATIBILITY_LEVEL_ERROR_CODE = 42203;
  public static final int INVALID_MODE_ERROR_CODE = 42204;
  public static final int OPERATION_NOT_PERMITTED_ERROR_CODE = 42205;
  public static final int REFERENCE_EXISTS_ERROR_CODE = 42206;
  public static final int ID_DOES_NOT_MATCH_ERROR_CODE = 42207;
  public static final int INVALID_SUBJECT_ERROR_CODE = 42208;
  public static final int SCHEMA_TOO_LARGE_ERROR_CODE = 42209;

  // HTTP 500
  public static final int STORE_ERROR_CODE = 50001;
  public static final int OPERATION_TIMEOUT_ERROR_CODE = 50002;
  public static final int REQUEST_FORWARDING_FAILED_ERROR_CODE = 50003;
  public static final int UNKNOWN_LEADER_ERROR_CODE = 50004;
  // 50005 is used by the RestService to indicate a JSON Parse Error

  public static RestException subjectNotFoundException(String subject) {
    return new RestNotFoundException(
        String.format(SUBJECT_NOT_FOUND_MESSAGE_FORMAT, subject), SUBJECT_NOT_FOUND_ERROR_CODE);
  }

  public static RestException subjectSoftDeletedException(String subject) {
    return new RestNotFoundException(
            String.format(SUBJECT_SOFT_DELETED_MESSAGE_FORMAT, subject), SUBJECT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException subjectNotSoftDeletedException(String subject) {
    return new RestNotFoundException(
            String.format(SUBJECT_NOT_SOFT_DELETED_MESSAGE_FORMAT, subject), SUBJECT_NOT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException schemaVersionSoftDeletedException(String subject, String version) {
    return new RestNotFoundException(
            String.format(SCHEMAVERSION_SOFT_DELETED_MESSAGE_FORMAT, subject, version),
            SCHEMAVERSION_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException schemaVersionNotSoftDeletedException(String subject, String version) {
    return new RestNotFoundException(
            String.format(SCHEMAVERSION_NOT_SOFT_DELETED_MESSAGE_FORMAT, subject, version),
            SCHEMAVERSION_NOT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException subjectLevelCompatibilityNotConfiguredException(String subject) {
    return new RestNotFoundException(
            String.format(SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_MESSAGE_FORMAT, subject),
            SUBJECT_LEVEL_COMPATIBILITY_NOT_CONFIGURED_ERROR_CODE);
  }

  public static RestException subjectLevelModeNotConfiguredException(String subject) {
    return new RestNotFoundException(
            String.format(SUBJECT_LEVEL_MODE_NOT_CONFIGURED_MESSAGE_FORMAT, subject),
            SUBJECT_LEVEL_MODE_NOT_CONFIGURED_ERROR_CODE);
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

  public static RestIncompatibleSchemaException incompatibleSchemaException(String message,
                                                                            Throwable cause) {
    return new RestIncompatibleSchemaException(message,
                                                   RestIncompatibleSchemaException.DEFAULT_ERROR_CODE,
                                                   cause);
  }

  public static RestIdDoesNotMatchException idDoesNotMatchException(Throwable cause) {
    return new RestIdDoesNotMatchException(cause.getMessage());
  }

  public static RestInvalidSchemaException invalidSchemaException(Throwable cause) {
    return new RestInvalidSchemaException(cause.getMessage());
  }

  public static RestInvalidVersionException invalidVersionException(String version) {
    return new RestInvalidVersionException(version);
  }

  public static RestInvalidSubjectException invalidSubjectException(String subject) {
    return new RestInvalidSubjectException(subject);
  }

  public static RestException schemaRegistryException(String message, Throwable cause) {
    return new RestSchemaRegistryException(message, cause);
  }

  public static RestException storeException(String message, Throwable cause) {
    return new RestSchemaRegistryStoreException(message, cause);
  }

  public static RestSchemaTooLargeException schemaTooLargeException(String message) {
    return new RestSchemaTooLargeException(message);
  }

  public static RestException operationTimeoutException(String message, Throwable cause) {
    return new RestSchemaRegistryTimeoutException(message, cause);
  }

  public static RestOperationNotPermittedException operationNotPermittedException(String message) {
    return new RestOperationNotPermittedException(message);
  }

  public static RestReferenceExistsException referenceExistsException(String message) {
    return new RestReferenceExistsException(message);
  }

  public static RestException requestForwardingFailedException(String message, Throwable cause) {
    return new RestRequestForwardingException(message, cause);
  }

  public static RestException unknownLeaderException(String message, Throwable cause) {
    return new RestUnknownLeaderException(message, cause);
  }
}
