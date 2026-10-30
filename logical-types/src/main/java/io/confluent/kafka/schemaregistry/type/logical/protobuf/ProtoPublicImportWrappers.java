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

package io.confluent.kafka.schemaregistry.type.logical.protobuf;

import com.google.protobuf.Descriptors.Descriptor;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.ProtobufMeta;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A Protobuf file declaring nothing but a public import (a public-import wrapper, as
 * {@link ProtobufSchema} calls it), as the file it re-exports: its records carry that file's
 * messages, and Flink reads that file's row. The converter keeps such a root as a bare reference
 * with no body, for {@code format=logical} and the logical-type writer, so provenance and the
 * LOGICAL policy resolve it here first.
 *
 * <p>Internal to Schema Registry: not a supported API, and it may change in any release.
 */
public final class ProtoPublicImportWrappers {

  private ProtoPublicImportWrappers() {
  }

  /**
   * The message a public-import wrapper's root param picks, the one its records carry, or null if
   * {@code schema} is no wrapper or has no such param.
   */
  public static String rootParam(ProtobufSchema schema) {
    ProtoFileElement own = schema.rawSchema();
    if (own == null || !own.getTypes().isEmpty() || own.getPublicImports().isEmpty()) {
      return null;
    }
    ProtobufMeta meta = ProtobufSchema.findMeta(ProtobufSchema.CONFLUENT_FILE_META,
        own.getOptions());
    return meta != null && meta.getParams() != null
        ? meta.getParams().get(ProtobufSchema.ROOT_KEY) : null;
  }

  /**
   * The file {@code schema} re-exports, converted as a direct registration of it would be, or
   * {@code schema} itself if it declares a type or no public import.
   *
   * @throws IllegalArgumentException if Protobuf cannot resolve the re-export, as for a chain of
   *     them or two public imports, which a serializer cannot use either
   * @throws ValidationException if a re-exported bundled file declares no message; a registered
   *     one is returned, and fails when converted as the file itself would
   */
  public static ProtobufSchema resolve(ProtobufSchema schema) {
    ProtoFileElement own = schema.rawSchema();
    if (own == null || !own.getTypes().isEmpty() || own.getPublicImports().isEmpty()) {
      return schema;
    }
    // Throws for a shape Protobuf resolves no further, as converting it would.
    Descriptor root = schema.toDescriptor();
    String target = own.getPublicImports().get(0);
    ProtoFileElement file = schema.dependencies().get(target);
    if (file == null) {
      // A built-in file re-exported directly, such as google/protobuf/type.proto: its own file,
      // its imports resolved as built-ins again, as the file registered directly would be.
      if (root == null) {
        throw new ValidationException("ProtobufSchema has no root descriptor");
      }
      return new ProtobufSchema(new ProtobufSchema(root).rawSchema(), Collections.emptyList(),
          Collections.emptyMap());
    }
    // The parsed file with the same other dependencies, so a well-known import inlines as in the
    // file itself; a descriptor would list it as a reference and make it a named type.
    List<SchemaReference> references = new ArrayList<>(schema.references());
    references.removeIf(reference -> target.equals(reference.getName()));
    Map<String, ProtoFileElement> dependencies = new LinkedHashMap<>(schema.dependencies());
    dependencies.remove(target);
    return new ProtobufSchema(file, references, dependencies);
  }
}
