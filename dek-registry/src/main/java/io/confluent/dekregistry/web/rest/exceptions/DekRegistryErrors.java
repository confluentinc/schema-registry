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

package io.confluent.dekregistry.web.rest.exceptions;

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;
import io.confluent.rest.exceptions.RestServerErrorException;

public class DekRegistryErrors {

  // HTTP 404
  public static final String KEY_NOT_FOUND_MESSAGE_FORMAT =
      "Key '%s' not found";
  public static final int KEY_NOT_FOUND_ERROR_CODE = 40470;

  public static final String KEY_NOT_SOFT_DELETED_MESSAGE_FORMAT = "Key '%s' was not deleted "
      + "first before being permanently deleted";
  public static final int KEY_NOT_SOFT_DELETED_ERROR_CODE = 40471;

  public static final String KEY_SOFT_DELETED_MESSAGE_FORMAT = "Key '%s' must be undeleted first";
  public static final int KEY_SOFT_DELETED_ERROR_CODE = 40472;

  // HTTP 409
  public static final String ALREADY_EXISTS_MESSAGE_FORMAT =
      "Key '%s' already exists; use a different value";
  public static final int ALREADY_EXISTS_ERROR_CODE = 40971;

  public static final String TOO_MANY_KEYS_MESSAGE_FORMAT =
      "A maximum of %d keys already exist";
  public static final int TOO_MANY_KEYS_ERROR_CODE = 40972;

  // HTTP 422
  public static final int INVALID_KEY_ERROR_CODE = 42271;
  public static final int REFERENCE_EXISTS_ERROR_CODE = 42272;

  // HTTP 500
  public static final int DEK_GENERATION_ERROR_CODE = 50070;

  public static RestException keyNotFoundException(String name) {
    return new RestNotFoundException(
        String.format(KEY_NOT_FOUND_MESSAGE_FORMAT, name), KEY_NOT_FOUND_ERROR_CODE);
  }

  public static RestException keyNotSoftDeletedException(String name) {
    return new RestNotFoundException(
        String.format(KEY_NOT_SOFT_DELETED_MESSAGE_FORMAT, name),
        KEY_NOT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException keySoftDeletedException(String name) {
    return new RestNotFoundException(
        String.format(KEY_SOFT_DELETED_MESSAGE_FORMAT, name),
        KEY_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException alreadyExistsException(String name) {
    return new RestConflictException(
        String.format(ALREADY_EXISTS_MESSAGE_FORMAT, name), ALREADY_EXISTS_ERROR_CODE);
  }

  public static RestException tooManyKeysException(int maxKeys) {
    return new RestConflictException(
        String.format(TOO_MANY_KEYS_MESSAGE_FORMAT, maxKeys),
        TOO_MANY_KEYS_ERROR_CODE);
  }

  public static RestInvalidKeyException invalidOrMissingKeyInfo(String field) {
    return new RestInvalidKeyException(field);
  }

  public static RestReferenceExistsException referenceExistsException(String name) {
    return new RestReferenceExistsException(name);
  }

  public static RestServerErrorException dekGenerationException(String message) {
    return new RestDekGenerationException(message);
  }
}
