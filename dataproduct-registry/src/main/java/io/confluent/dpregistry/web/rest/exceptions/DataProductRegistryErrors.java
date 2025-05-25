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

package io.confluent.dpregistry.web.rest.exceptions;

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class DataProductRegistryErrors {

  // HTTP 404
  public static final String DATA_PRODUCT_NOT_FOUND_MESSAGE_FORMAT =
      "Data product '%s' not found";
  public static final int DATA_PRODUCT_NOT_FOUND_ERROR_CODE = 40470;

  public static final String DATA_PRODUCT_NOT_SOFT_DELETED_MESSAGE_FORMAT =
      "Data product '%s' was not deleted first before being permanently deleted";
  public static final int DATA_PRODUCT_NOT_SOFT_DELETED_ERROR_CODE = 40471;

  public static final String DATA_PRODUCT_SOFT_DELETED_MESSAGE_FORMAT =
      "Key '%s' must be undeleted first";
  public static final int DATA_PRODUCT_SOFT_DELETED_ERROR_CODE = 40472;

  // HTTP 409
  public static final String ALREADY_EXISTS_MESSAGE_FORMAT =
      "Data product '%s' already exists; use a different value";
  public static final int ALREADY_EXISTS_ERROR_CODE = 40971;

  // HTTP 422
  public static final int INVALID_DATA_PRODUCT_ERROR_CODE = 42271;

  public static RestException dataProductNotFoundException(String name) {
    return new RestNotFoundException(
        String.format(DATA_PRODUCT_NOT_FOUND_MESSAGE_FORMAT, name),
        DATA_PRODUCT_NOT_FOUND_ERROR_CODE);
  }

  public static RestException dataProductNotSoftDeletedException(String name) {
    return new RestNotFoundException(
        String.format(DATA_PRODUCT_NOT_SOFT_DELETED_MESSAGE_FORMAT, name),
        DATA_PRODUCT_NOT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException dataProductSoftDeletedException(String name) {
    return new RestNotFoundException(
        String.format(DATA_PRODUCT_SOFT_DELETED_MESSAGE_FORMAT, name),
        DATA_PRODUCT_SOFT_DELETED_ERROR_CODE);
  }

  public static RestException alreadyExistsException(String name) {
    return new RestConflictException(
        String.format(ALREADY_EXISTS_MESSAGE_FORMAT, name), ALREADY_EXISTS_ERROR_CODE);
  }

  public static RestInvalidDataProductException invalidOrMissingDataProduct(String field) {
    return new RestInvalidDataProductException(field);
  }
}
