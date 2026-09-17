/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical;

import io.confluent.kafka.schemaregistry.AbstractSchemaProvider;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import java.util.Map;
import java.util.Optional;

/**
 * A schema provider that also accepts schemas written as logical types DDL, converting them to
 * the native format of the provider it wraps.
 *
 * <p>Native-first, the same rule the registry applies: the body is handed to the wrapped provider
 * first, and only a body that provider rejects is tried as DDL. So a native schema always wins,
 * and a body that is neither reports the native error rather than a misleading DDL one.
 *
 * <p>Note that this converts locally. A caller that wants the registry to do the conversion --
 * so that what it stores is decided server-side -- must send the DDL itself rather than the
 * schema this returns.
 */
public class LogicalSchemaProvider extends AbstractSchemaProvider {

  private final SchemaProvider delegate;

  public LogicalSchemaProvider(SchemaProvider delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns whether {@code text} is logical types DDL, without converting it.
   */
  public static boolean isLogical(String text) {
    return LogicalTypeConversion.tryParse(text).isPresent();
  }

  @Override
  public String schemaType() {
    return delegate.schemaType();
  }

  /**
   * Only a DDL body reads the subject, and then only to name a root the script leaves anonymous.
   * A native body is handed to the delegate untouched, so it stays cacheable by content alone.
   */
  @Override
  public boolean isSubjectDependent(Schema schema) {
    return isLogical(schema.getSchema());
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // Both halves need configuring: the delegate parses native bodies, while this class resolves
    // the references of a DDL body through the version fetcher the base class captures.
    super.configure(configs);
    delegate.configure(configs);
  }

  @Override
  public ParsedSchema parseSchemaOrElseThrow(
      Schema schema, boolean validateAsNew, boolean normalize) {
    try {
      return delegate.parseSchemaOrElseThrow(schema, validateAsNew, normalize);
    } catch (RuntimeException e) {
      Optional<LogicalTypesParser.ScriptContext> script =
          LogicalTypeConversion.tryParse(schema.getSchema());
      if (!script.isPresent()) {
        // Neither native nor DDL: the native failure is the one worth reporting.
        throw e;
      }
      return LogicalTypeConversion.toNative(script.get(), schema, resolveReferences(schema));
    }
  }

  @Override
  protected Map<String, String> resolveReferences(Schema schema) {
    try {
      return super.resolveReferences(schema);
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new ValidationException("Could not resolve schema references: " + e.getMessage(), e);
    }
  }
}
