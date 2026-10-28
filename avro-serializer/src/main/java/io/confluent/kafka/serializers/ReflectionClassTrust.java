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

package io.confluent.kafka.serializers;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.utils.BoundedConcurrentHashMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.specific.AvroGenerated;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The classes that a reflection deserializer may load when a schema names them, configured as
 * with Avro 1.12.2's class validator (AVRO-4189), which replaces this. Unconfigured, it only warns.
 */
final class ReflectionClassTrust {

  private static final Logger log = LoggerFactory.getLogger(ReflectionClassTrust.class);

  private static final String ALL_PACKAGES = "*";
  private static final int MAX_REMEMBERED = 1000;

  private final Set<String> classes = new HashSet<>();
  private final Set<String> packages = new HashSet<>();
  private final boolean allPackages;
  private final boolean configured;
  private final Set<String> warned =
      Collections.newSetFromMap(new BoundedConcurrentHashMap<>(MAX_REMEMBERED));
  // Registry schemas already checked, by identity
  private final Cache<Schema, Boolean> checkedSchemas = CacheBuilder.newBuilder()
      .weakKeys()
      .maximumSize(MAX_REMEMBERED)
      .build();

  ReflectionClassTrust() {
    this(Collections.emptyList(), Collections.emptyList());
  }

  ReflectionClassTrust(List<String> classNames, List<String> packageNames) {
    for (String className : classNames) {
      if (className != null && !className.trim().isEmpty()) {
        classes.add(className.trim());
      }
    }
    boolean all = false;
    for (String packageName : packageNames) {
      if (packageName == null || packageName.trim().isEmpty()) {
        continue;
      }
      String trusted = packageName.trim();
      if (ALL_PACKAGES.equals(trusted)) {
        all = true;
      } else {
        // "com.example." means "com.example"
        packages.add(trusted.endsWith(".") ? trusted.substring(0, trusted.length() - 1) : trusted);
      }
    }
    allPackages = all;
    configured = allPackages || !classes.isEmpty() || !packages.isEmpty();
    if (allPackages) {
      log.warn("Trusting every class that a schema names for reflection deserialization");
    }
  }

  // The class ReflectData.getClass resolves for a writer schema: through arrays and unions with
  // null, but not record fields, whose types come from the resolved class
  void checkClassResolvedBy(Schema writerSchema) {
    switch (writerSchema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        check(writerSchema);
        break;
      case ARRAY:
        checkClassResolvedBy(writerSchema.getElementType());
        break;
      case UNION:
        List<Schema> types = writerSchema.getTypes();
        if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL) {
          checkClassResolvedBy(types.get(1));
        } else if (types.size() == 2 && types.get(1).getType() == Schema.Type.NULL) {
          checkClassResolvedBy(types.get(0));
        }
        break;
      default:
        break;
    }
  }

  // Every named type in a reader schema from the registry, since a reflect reader builds them all
  void checkClassesNamedBy(Schema readerSchema) {
    if (checkedSchemas.getIfPresent(readerSchema) == null) {
      checkClassesNamedBy(readerSchema, Collections.newSetFromMap(new IdentityHashMap<>()));
      checkedSchemas.put(readerSchema, Boolean.TRUE);
    }
  }

  private void checkClassesNamedBy(Schema schema, Set<Schema> seen) {
    if (!seen.add(schema)) {
      return;
    }
    switch (schema.getType()) {
      case RECORD:
        check(schema);
        for (Schema.Field field : schema.getFields()) {
          checkClassesNamedBy(field.schema(), seen);
        }
        break;
      case ENUM:
      case FIXED:
        check(schema);
        break;
      case ARRAY:
        checkClassesNamedBy(schema.getElementType(), seen);
        break;
      case MAP:
        checkClassesNamedBy(schema.getValueType(), seen);
        break;
      case UNION:
        for (Schema type : schema.getTypes()) {
          checkClassesNamedBy(type, seen);
        }
        break;
      default:
        break;
    }
  }

  private void check(Schema schema) {
    // Found without initializing it
    Class<?> clazz = AvroSchemaUtils.findSpecificClass(schema);
    if (clazz == null || isTrusted(clazz)) {
      return;
    }
    if (configured) {
      throw new SerializationException(clazz.getName() + " named by the schema is not trusted "
          + "for reflection deserialization; add it to "
          + KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG + " or its package to "
          + KafkaAvroDeserializerConfig.AVRO_TRUSTED_PACKAGES_CONFIG);
    }
    if (warned.add(clazz.getName())) {
      log.warn("Loading {}, which a schema names, for reflection deserialization. Add it to {} "
          + "or its package to {}; a future release will require this.", clazz.getName(),
          KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG,
          KafkaAvroDeserializerConfig.AVRO_TRUSTED_PACKAGES_CONFIG);
    }
  }

  // Package-private for tests
  boolean isTrusted(Class<?> clazz) {
    // Reading an annotation does not initialize the class
    if (allPackages || classes.contains(clazz.getName())
        || clazz.isAnnotationPresent(AvroGenerated.class)) {
      return true;
    }
    // A nested class reports its enclosing package
    Package classPackage = clazz.getPackage();
    if (classPackage == null) {
      return false;
    }
    String packageName = classPackage.getName();
    for (String trusted : packages) {
      // "com.example" also trusts its subpackages, but not "com.examplex"
      if (packageName.startsWith(trusted) && (packageName.length() == trusted.length()
          || packageName.charAt(trusted.length()) == '.')) {
        return true;
      }
    }
    return false;
  }
}
