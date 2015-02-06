/**
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
 **/

package io.confluent.kafka.schemaregistry.exceptions;

/**
 * Indicates that there was an attempt to set a SchemaRegistry ineligible for election
 * (masterEligibility == false) to master.
 */
public class SchemaRegistryIneligibleMasterException extends SchemaRegistryException {
  public SchemaRegistryIneligibleMasterException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaRegistryIneligibleMasterException(String message) {
    super(message);
  }

  public SchemaRegistryIneligibleMasterException(Throwable cause) {
    super(cause);
  }

  public SchemaRegistryIneligibleMasterException() {
    super();
  }
}
