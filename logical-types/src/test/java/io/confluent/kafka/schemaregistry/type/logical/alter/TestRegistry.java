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

package io.confluent.kafka.schemaregistry.type.logical.alter;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.SchemaType;
import io.confluent.kafka.schemaregistry.type.logical.provenance.ProvenanceMockSchemaRegistryClient;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * A mock registry that takes logical types DDL in all three formats, as the registry does, and
 * answers this package's lookups as a client of one would.
 */
final class TestRegistry implements TypeLookup {

  final SchemaRegistryClient client = new ProvenanceMockSchemaRegistryClient(List.of(
      new LogicalSchemaProvider(new AvroSchemaProvider()),
      new LogicalSchemaProvider(new ProtobufSchemaProvider()),
      new LogicalSchemaProvider(new JsonSchemaProvider())));

  /** Creates a type named as its subject, as a CREATE TYPE statement would. */
  LogicalType create(String subject, SchemaType format, TypeScripts.BodyKind kind, String body) {
    return create(subject, subject, format, kind, body);
  }

  /**
   * Registers a type under a subject and returns it. Its dotted name's last part is the name
   * its schema declares, in the namespace the parts before it make.
   */
  LogicalType create(String subject, String name, SchemaType format, TypeScripts.BodyKind kind,
      String body) {
    int dot = name.lastIndexOf('.');
    LogicalType parsed = TypeReferences.parse(this, format, aliases -> TypeScripts.createScript(
        name.substring(dot + 1), kind, body, Map.of(), aliases), subject, Map.of());
    LogicalType lt = kind != TypeScripts.BodyKind.OTHER && dot > 0
        ? TypeScripts.withNamespace(parsed, name.substring(0, dot)) : parsed;
    Formats.refuseUnwritable(format, lt);
    try {
      client.registerWithRequestResponse(subject, Formats.request(format, lt), false);
    } catch (IOException | RestClientException e) {
      throw new IllegalStateException(e);
    }
    return lt;
  }

  @Override
  public SchemaMetadata latest(String subject) {
    try {
      return client.getLatestSchemaMetadata(subject);
    } catch (RestClientException e) {
      if (e.getStatus() == 404) {
        return null;
      }
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public SchemaMetadata version(String subject, int version) {
    try {
      return client.getSchemaMetadata(subject, version, true);
    } catch (RestClientException e) {
      if (e.getStatus() == 404) {
        return null;
      }
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public ParsedSchema parse(SchemaMetadata md) {
    return client.parseSchema(new Schema(md.getSubject(), md.getVersion(), md.getId(),
        md.getSchemaType() == null ? "AVRO" : md.getSchemaType(), md.getReferences(),
        md.getSchema())).orElseThrow(() -> new IllegalStateException(
            "Could not parse schema " + md.getId()));
  }
}
