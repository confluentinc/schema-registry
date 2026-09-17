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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.avro.LogicalTypeToAvroConverter;
import io.confluent.kafka.schemaregistry.type.logical.generated.LogicalTypesParser;
import io.confluent.kafka.schemaregistry.type.logical.json.LogicalTypeToJsonConverter;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.LogicalTypeToProtoConverter;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
   *
   * <p>This only decides the form of the body. A script that parses but is semantically invalid
   * is still DDL, and says so when converted.
   */
  public static boolean isLogical(String text) {
    if (text == null || text.trim().isEmpty()) {
      return false;
    }
    try {
      LogicalTypesParserFactory.parse(text);
      return true;
    } catch (RuntimeException e) {
      return false;
    }
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
      LogicalTypesParser.ScriptContext script;
      try {
        script = LogicalTypesParserFactory.parse(schema.getSchema());
      } catch (RuntimeException notDdl) {
        // Neither native nor DDL: the native failure is the one worth reporting.
        throw e;
      }
      return toNative(schema, attachReferences(schema, toLogicalType(script)));
    }
  }

  /**
   * A logical type carries external-type bindings but never registry coordinates, so a type that
   * references anything external is resolved against the caller-declared references the way a
   * native schema's are, and the resolved definitions are attached for the conversion to emit.
   */
  private LogicalType attachReferences(Schema schema, LogicalType parsed) {
    List<SchemaReference> references = schema.getReferences();
    if (references == null || references.isEmpty()) {
      return parsed;
    }
    Map<String, String> resolvedReferences;
    try {
      resolvedReferences = resolveReferences(schema);
    } catch (IllegalArgumentException | IllegalStateException e) {
      throw new ValidationException("Could not resolve schema references: " + e.getMessage(), e);
    }
    return new LogicalType(
        parsed.getName(),
        parsed.getNamespace(),
        parsed.getRootSchema(),
        parsed.getNamedTypes(),
        parsed.getExternalTypes(),
        parsed.getExternalImports(),
        references,
        resolvedReferences,
        parsed.getDefaultValues());
  }

  /**
   * Syntax decides logical-vs-native; semantics decides valid-vs-invalid. A script that parses
   * and then fails the visitor is a bad logical schema and must say so, rather than falling back
   * to a confusing native error.
   */
  private LogicalType toLogicalType(LogicalTypesParser.ScriptContext script) {
    try {
      LogicalTypesSchemaVisitor visitor = new LogicalTypesSchemaVisitor();
      visitor.visit(script);
      return visitor.toLogicalType();
    } catch (RuntimeException e) {
      throw new ValidationException("Invalid logical type schema: " + e.getMessage(), e);
    }
  }

  /**
   * External imports are left to the converters, which decide per format: they are a JSON-only
   * construct, so the Avro and Protobuf converters reject a type that carries them.
   */
  private ParsedSchema toNative(Schema schema, LogicalType logicalType) {
    String rowName = rowName(schema.getSubject());
    try {
      switch (schemaType().toUpperCase(Locale.ROOT)) {
        case AvroSchema.TYPE:
          return withRequested(
              LogicalTypeToAvroConverter.fromLogicalType(logicalType, rowName), schema);
        case JsonSchema.TYPE:
          return withRequested(
              LogicalTypeToJsonConverter.fromLogicalType(logicalType, rowName), schema);
        case ProtobufSchema.TYPE:
          return withRequested(
              LogicalTypeToProtoConverter.fromLogicalType(logicalType, rowName), schema);
        default:
          throw new ValidationException(
              "Unsupported schemaType '" + schemaType() + "' for a logical type schema; "
                  + "must be one of " + AvroSchema.TYPE + ", " + JsonSchema.TYPE + ", "
                  + ProtobufSchema.TYPE);
      }
    } catch (ValidationException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new ValidationException(
          "Logical type schema cannot be represented as " + schemaType() + ": " + e.getMessage(),
          e);
    }
  }

  /**
   * Carries the requested metadata and rule set onto the converted schema, the way a native
   * provider carries them onto what it parses. The conversion sets metadata of its own, so the
   * two are merged rather than replaced, with the request winning on a conflicting property.
   */
  private static ParsedSchema withRequested(ParsedSchema converted, Schema schema) {
    if (schema.getMetadata() == null && schema.getRuleSet() == null) {
      return converted;
    }
    Metadata metadata = Metadata.mergeMetadata(converted.metadata(), schema.getMetadata());
    RuleSet ruleSet = schema.getRuleSet() != null ? schema.getRuleSet() : converted.ruleSet();
    return converted.copy(metadata, ruleSet);
  }

  /**
   * Derives the name of the converted root record from the subject, matching how the registry
   * names it. The subject is absent on the entry points that take a bare schema string, in which
   * case the name falls back to the same default the registry uses for an empty subject.
   */
  private static String rowName(String subject) {
    String sanitized = subject == null ? "" : subject.replaceAll("[^A-Za-z0-9_]", "_");
    if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
      sanitized = "Envelope" + sanitized;
    }
    return sanitized;
  }
}
