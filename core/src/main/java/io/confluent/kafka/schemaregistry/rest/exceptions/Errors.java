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

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class Errors {

  public final static String SUBJECT_NOT_FOUND_MESSAGE = "Subject not found.";
  public final static int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;

  public static RestException subjectNotFoundException() {
    return new RestNotFoundException(SUBJECT_NOT_FOUND_MESSAGE, SUBJECT_NOT_FOUND_ERROR_CODE);
  }

  public final static String VERSION_NOT_FOUND_MESSAGE = "Version not found.";
  public final static int VERSION_NOT_FOUND_ERROR_CODE = 40402;

  public static RestException versionNotFoundException() {
    return new RestNotFoundException(VERSION_NOT_FOUND_MESSAGE, VERSION_NOT_FOUND_ERROR_CODE);
  }
  
  public final static String SCHEMA_NOT_FOUND_MESSAGE = "Schema not found";
  public final static int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
  
  public static RestException schemaNotFoundException() {
    return new RestNotFoundException(SCHEMA_NOT_FOUND_MESSAGE, SCHEMA_NOT_FOUND_ERROR_CODE);
  }
}
