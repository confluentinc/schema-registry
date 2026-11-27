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

package io.confluent.kafka.schemaregistry.type.logical.statements;

import static org.assertj.core.api.Assertions.assertThat;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
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
import java.util.regex.Pattern;

/**
 * A mock registry that takes logical types DDL in all three formats, as the registry does, and
 * answers this package's lookups as a client of one would.
 */
final class TestRegistry implements TypeLookup {

  // Adds an attribute to a type and tells that the schema written has each of these.
  void alteredTo(String subject, String... has) {
    int was = latest(subject).getVersion();
    alter(subject, new AlterAction.AddAttribute("z", "INT"));
    assertThat(latest(subject).getVersion()).as(subject).isEqualTo(was + 1);
    assertThat(schema(subject).canonicalString()).as(subject).contains(has);
  }

  // A name a JSON type has for what it refers to, as the writer writes it.
  static String alias(String name, String to) {
    return "\"" + name + "\":{\"$ref\":\"" + to + "\",\"logical.ref\":true}";
  }

  // The refusal of a schema two places of which are read by one name, as a pattern: the
  // two are told in the order the reader comes to them.
  static String readAsOne(String done, String name, String one, String other) {
    String both = Pattern.quote(one) + " and to " + Pattern.quote(other);
    String turned = Pattern.quote(other) + " and to " + Pattern.quote(one);
    return "(?s)Type \\S+ cannot be " + done + ": it refers to (" + both + "|" + turned
        + "), which are both read by the name " + Pattern.quote(name) + "\\. The two are read "
        + "as one, and the one would be written as the other\\. Register its new version as "
        + "a JSON schema";
  }

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
   * Creates a type under a subject, as a CREATE TYPE statement would, and returns it. Its dotted
   * name's last part is the name its schema declares, in the namespace the parts before it make.
   */
  LogicalType create(String subject, String name, SchemaType format, TypeScripts.BodyKind kind,
      String body) {
    return register(subject, TypeCreate.create(this, subject, name, kind, body, "",
        Map.of(TypeAlter.FORMAT, format.name()), null)).edited();
  }

  /**
   * Alters a type as an ALTER TYPE statement would: the package makes the new version, and
   * setting the policy and registering it are the tool's.
   */
  TypeAlter.Result alter(String subject, AlterAction... actions) {
    return alter(subject, new TypeAlter.Options(), actions);
  }

  TypeAlter.Result alter(String subject, TypeAlter.Options options, AlterAction... actions) {
    return register(subject, TypeAlter.alter(this, subject, List.of(actions), options));
  }

  /** What a tool does with a result: sets the policy it has, then registers its version. */
  TypeAlter.Result register(String subject, TypeAlter.Result result) {
    try {
      if (result.policy() != null) {
        Config config = new Config((String) null);
        config.setCompatibilityPolicy(result.policy());
        client.updateConfig(subject, config);
      }
      RegisterSchemaRequest request = result.request();
      if (request != null) {
        // As the registry does: a request with no metadata or rule set of its own takes the
        // latest version's.
        SchemaMetadata latest = latest(subject);
        if (latest != null && request.getMetadata() == null) {
          request.setMetadata(latest.getMetadata());
        }
        if (latest != null && request.getRuleSet() == null) {
          request.setRuleSet(latest.getRuleSet());
        }
        client.registerWithRequestResponse(subject, request, false);
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

  /**
   * Soft-deletes every live version of a subject, as a DROP TYPE does; one by one, since the
   * mock forgets the versions of a subject deleted whole.
   */
  void drop(String subject) {
    versions(subject, false).forEach(v -> softDelete(subject, v));
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

  // As the registry renders format=logical: the stored schema as logical types DDL.
  @Override
  public LogicalType logicalType(String subject, SchemaMetadata md) {
    return TypeReferences.read(this, subject, md,
        LogicalTypeToDdlConverter.toDdl(LogicalTypeConversion.toLogicalType(parse(md))));
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
        md.getMetadata(), md.getRuleSet(), md.getSchema())).orElseThrow(() -> new IllegalStateException(
            "Could not parse schema " + md.getId()));
  }
}
