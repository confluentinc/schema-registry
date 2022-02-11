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

import io.confluent.kafka.schemaregistry.exceptions.IdDoesNotMatchException;
import io.confluent.kafka.schemaregistry.exceptions.IdGenerationException;
import io.confluent.kafka.schemaregistry.exceptions.IncompatibleSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.OperationNotPermittedException;
import io.confluent.kafka.schemaregistry.exceptions.ReferenceExistsException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaVersionNotSoftDeletedException;
import io.confluent.kafka.schemaregistry.exceptions.SubjectNotSoftDeletedException;
import io.confluent.rest.exceptions.RestServerErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;

/**
 * Indicates some error while performing a schema registry operation
 */
public class RestSchemaRegistryException extends RestServerErrorException {

  private static final int ERROR_CODE = RestServerErrorException.DEFAULT_ERROR_CODE;

  private static final Logger LOG = LoggerFactory.getLogger(RestSchemaRegistryException.class);

  private Collection<Class> forbiddenClasses =
          Arrays.asList(IdDoesNotMatchException.class, IdGenerationException.class, IncompatibleSchemaException.class, InvalidSchemaException.class, InvalidVersionException.class, OperationNotPermittedException.class, ReferenceExistsException.class, SchemaVersionNotSoftDeletedException.class, SubjectNotSoftDeletedException.class);

  public RestSchemaRegistryException(String message) {
    super(message, ERROR_CODE);
  }

  public RestSchemaRegistryException(String message, Throwable cause) {
    super(message, ERROR_CODE, cause);
    if (forbiddenClasses.stream().anyMatch(clazz -> clazz.isInstance(cause))) {
      final String msg = "DMISCA Invalid Error: " + cause;
      LOG.error(msg, new Exception(msg, cause));
    }
  }
}
