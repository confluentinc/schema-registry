/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.web.rest.exceptions;

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class DekRegistryErrors {

  // HTTP 404
  public static final String KEY_NOT_FOUND_MESSAGE_FORMAT =
      "Key '%s' not found.";
  public static final int KEY_NOT_FOUND_ERROR_CODE = 40470;

  public static final String KEY_NOT_SOFT_DELETED_MESSAGE_FORMAT = "Key '%s' was not deleted "
      + "first before being permanently deleted";
  public static final int KEY_NOT_SOFT_DELETED_ERROR_CODE = 40471;

  // HTTP 409
  public static final String ALREADY_EXISTS_MESSAGE_FORMAT =
      "Key '%s' already exists.";
  public static final int ALREADY_EXISTS_ERROR_CODE = 40971;

  public static final String TOO_MANY_KEYS_MESSAGE_FORMAT =
      "A maximum of %d keys already exist.";
  public static final int TOO_MANY_KEYS_ERROR_CODE = 40972;

  // HTTP 422
  public static final int INVALID_KEY_ERROR_CODE = 42271;
  public static final int REFERENCE_EXISTS_ERROR_CODE = 42272;

  public static RestException keyNotFoundException(String name) {
    return new RestNotFoundException(
        String.format(KEY_NOT_FOUND_MESSAGE_FORMAT, name), KEY_NOT_FOUND_ERROR_CODE);
  }

  public static RestException keyNotSoftDeletedException(String name) {
    return new RestNotFoundException(
        String.format(KEY_NOT_SOFT_DELETED_MESSAGE_FORMAT, name),
        KEY_NOT_SOFT_DELETED_ERROR_CODE);
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

  public static RestInvalidKeyException invalidOrMissingKeyInfo(String message) {
    return new RestInvalidKeyException(message);
  }

  public static RestReferenceExistsException referenceExistsException(String message) {
    return new RestReferenceExistsException(message);
  }
}
