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
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeConversion;
import io.confluent.kafka.schemaregistry.type.logical.LogicalTypeToDdlConverter;
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
      new LogicalSchemaProvider(new JsonSchemaProvider()))) {

    // As the registry resolves a stored schema's reference to a soft-deleted version.
    @Override
    public Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
      try {
        return super.getByVersion(subject, version, lookupDeletedSchema);
      } catch (RuntimeException e) {
        if (!lookupDeletedSchema || version < 0) {
          throw e;
        }
        try {
          SchemaMetadata md = getSchemaMetadata(subject, version, true);
          return new Schema(subject, md.getVersion(), md.getId(), md.getSchemaType(),
              md.getReferences(), md.getSchema());
        } catch (IOException | RestClientException notFound) {
          throw e;
        }
      }
    }
  };

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
    LogicalType lt = Constraints.named(kind != TypeScripts.BodyKind.OTHER && dot > 0
        ? TypeScripts.withNamespace(parsed, name.substring(0, dot)) : parsed, subject);
    Formats.refuseUnwritable(format, lt);
    try {
      client.registerWithRequestResponse(subject, Formats.request(format, lt), false);
    } catch (IOException | RestClientException e) {
      throw new IllegalStateException(e);
    }
    return lt;
  }

  /**
   * Alters a type as an ALTER TYPE statement would: the package makes the new version, and
   * setting the policy and registering it are the tool's.
   */
  TypeAlter.Result alter(String subject, AlterAction... actions) {
    return alter(subject, new TypeAlter.Options(), actions);
  }

  TypeAlter.Result alter(String subject, TypeAlter.Options options, AlterAction... actions) {
    TypeAlter.Result result = TypeAlter.alter(this, subject, List.of(actions), options);
    try {
      if (result.policy() != null) {
        Config config = new Config((String) null);
        config.setCompatibilityPolicy(result.policy());
        client.updateConfig(subject, config);
      }
      if (result.request() != null) {
        client.registerWithRequestResponse(subject, result.request(), false);
      }
    } catch (IOException | RestClientException e) {
      throw new IllegalStateException(e);
    }
    return result;
  }

  /** The latest version's native schema. */
  ParsedSchema schema(String subject) {
    return parse(latest(subject));
  }

  /** The latest version's root struct, read back from what was registered. */
  io.confluent.kafka.schemaregistry.type.logical.Schema struct(String subject) {
    LogicalType lt = LogicalTypeConversion.toLogicalType(schema(subject));
    io.confluent.kafka.schemaregistry.type.logical.Schema root = lt.getRootSchema();
    return root.getType() == io.confluent.kafka.schemaregistry.type.logical.Schema.Type
        .NAMED_TYPE_REF ? lt.getNamedTypes().get(root.getQualifiedName()) : root;
  }

  void softDelete(String subject, int version) {
    try {
      client.deleteSchemaVersion(subject, String.valueOf(version));
    } catch (IOException | RestClientException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public List<Integer> versions(String subject, boolean deleted) {
    try {
      return client.getAllVersions(subject, deleted);
    } catch (RestClientException e) {
      if (e.getStatus() == 404) {
        return List.of();
      }
      throw new IllegalStateException(e);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  // As the registry renders format=logical: the stored schema as logical types DDL, read back
  // with its references.
  @Override
  public LogicalType logicalType(String subject, SchemaMetadata md) {
    String ddl = LogicalTypeToDdlConverter.toDdl(LogicalTypeConversion.toLogicalType(parse(md)));
    List<SchemaReference> references =
        md.getReferences() != null ? md.getReferences() : List.of();
    boolean protobuf = Formats.ofSchemaType(md.getSchemaType()) == SchemaType.PROTOBUF;
    return TypeScripts.withReferences(TypeScripts.parse(ddl, protobuf), references,
        TypeReferences.resolve(this, subject, references));
  }

  // As the registry gives a subject's settings in scope: each unset one the global one.
  @Override
  public Config config(String subject) {
    try {
      return Config.mergeConfigs(client.getConfig(null), client.getConfig(subject));
    } catch (RestClientException e) {
      if (e.getStatus() != 404) {
        throw new IllegalStateException(e);
      }
      try {
        return client.getConfig(null);
      } catch (IOException | RestClientException global) {
        throw new IllegalStateException(global);
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
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
