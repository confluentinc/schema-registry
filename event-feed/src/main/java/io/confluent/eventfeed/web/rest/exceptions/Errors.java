/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.eventfeed.web.rest.exceptions;

public class Errors {

  // HTTP 422
  public static final int INVALID_CLOUD_EVENT_ERROR_CODE = 42270;
  public static final int UNSUPPORTED_CLOUD_EVENT_ERROR_CODE = 42271;

  // HTTP 500
  public static final int STORE_ERROR_CODE = 50070;
  public static final int STORE_TIMEOUT_ERROR_CODE = 50071;

  public static RestInvalidCloudEventException invalidCloudEventException(String message) {
    return new RestInvalidCloudEventException(message);
  }

  public static RestUnsupportedCloudEventException unsupportedCloudEventException(String message) {
    return new RestUnsupportedCloudEventException(message);
  }

  public static RestStoreException storeException(String message) {
    return new RestStoreException(message);
  }

  public static RestStoreException storeException(String message, Throwable cause) {
    return new RestStoreException(message, cause);
  }

  public static RestStoreTimeoutException storeTimeoutException(String message) {
    return new RestStoreTimeoutException(message);
  }

  public static RestStoreTimeoutException storeTimeoutException(String message, Throwable cause) {
    return new RestStoreTimeoutException(message, cause);
  }

  public static RestEventFeedException eventFeedException(String message, Throwable cause) {
    return new RestEventFeedException(message, cause);
  }
}
