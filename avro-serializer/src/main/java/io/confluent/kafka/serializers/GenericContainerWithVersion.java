/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.kafka.serializers;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.RuleResult;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.avro.generic.GenericContainer;

/**
 * Wrapper for GenericContainer along with a version number, which may be null.
 *
 * <p>The version is typically the version of the Avro schema of the GenericContainer in the context
 * of a subject.  The version is used to set the version on the Connect Schema that is
 * derived from the Avro schema, but only if the Avro schema does not have a property named
 * "connect.version", which takes precedence over the version here.
 */
public class GenericContainerWithVersion implements ParsedSchemaAndValue {

  private final AvroSchema schema;
  private final GenericContainer container;
  private final Integer version;
  private final SchemaInfo writerSchemaInfo;
  private final AvroSchema writerSchema;
  private final List<RuleResult> ruleResults;

  public GenericContainerWithVersion(GenericContainer container, Integer version) {
    this(new AvroSchema(container.getSchema()), container, version, null, null,
        Collections.emptyList());
  }

  public GenericContainerWithVersion(
      AvroSchema schema, GenericContainer container, Integer version) {
    this(schema, container, version, null, null, Collections.emptyList());
  }

  public GenericContainerWithVersion(
      AvroSchema schema,
      GenericContainer container,
      Integer version,
      SchemaInfo writerSchemaInfo,
      AvroSchema writerSchema,
      List<RuleResult> ruleResults) {
    this.schema = schema;
    this.container = container;
    this.version = version;
    this.writerSchemaInfo = writerSchemaInfo;
    this.writerSchema = writerSchema;
    this.ruleResults = ruleResults != null ? ruleResults : Collections.emptyList();
  }

  @Override
  public ParsedSchema getSchema() {
    return schema;
  }

  @Override
  public Object getValue() {
    return container;
  }

  @Override
  public SchemaInfo getWriterSchemaInfo() {
    return writerSchemaInfo;
  }

  @Override
  public ParsedSchema getWriterSchema() {
    return writerSchema;
  }

  @Override
  public List<RuleResult> getRuleResults() {
    return ruleResults;
  }

  public GenericContainer container() {
    return container;
  }

  public Integer version() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericContainerWithVersion that = (GenericContainerWithVersion) o;
    return Objects.equals(container, that.container) && Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(container, version);
  }
}
