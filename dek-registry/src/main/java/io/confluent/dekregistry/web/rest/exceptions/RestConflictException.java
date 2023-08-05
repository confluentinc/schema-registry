/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.web.rest.exceptions;

import io.confluent.rest.exceptions.RestException;
import javax.ws.rs.core.Response;

public class RestConflictException extends RestException {

  public static final int DEFAULT_ERROR_CODE =
      Response.Status.CONFLICT.getStatusCode();

  public RestConflictException(String message, int errorCode) {
    this(message, errorCode, null);
  }

  public RestConflictException(String message, int errorCode, Throwable cause) {
    super(message, DEFAULT_ERROR_CODE, errorCode, cause);
  }
}
