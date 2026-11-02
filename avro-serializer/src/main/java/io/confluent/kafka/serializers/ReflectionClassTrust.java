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
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.AvroGenerated;
import org.apache.avro.specific.SpecificData;
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
  // Avro 1.12.2's default trusted classes, and the collections that the upgrade adds
  private static final Set<String> DEFAULT_CLASSES = new HashSet<>(Arrays.asList(
      "java.lang.Boolean", "java.lang.Byte", "java.lang.Character", "java.lang.CharSequence",
      "java.lang.Double", "java.lang.Enum", "java.lang.Float", "java.lang.Integer",
      "java.lang.Long", "java.lang.Number", "java.lang.Object", "java.lang.Short",
      "java.lang.String", "java.lang.Void", "java.math.BigDecimal", "java.math.BigInteger",
      "java.util.Collection", "java.util.List", "java.util.ArrayList", "java.util.Set",
      "java.util.HashSet", "java.util.LinkedHashSet", "java.util.TreeSet", "java.util.Map",
      "java.util.HashMap", "java.util.LinkedHashMap", "java.util.TreeMap",
      "java.util.concurrent.ConcurrentHashMap"));
  private static final int MAX_REMEMBERED = 1000;

  private final Set<String> classes = new HashSet<>();
  private final Set<String> packages = new HashSet<>();
  private final boolean allPackages;
  private final boolean configured;
  private final Set<String> warned =
      Collections.newSetFromMap(new BoundedConcurrentHashMap<>(MAX_REMEMBERED));
  // Registry schemas already checked, by identity, with the context class loader they were
  // checked under
  private final Cache<Schema, CheckedFor> checkedSchemas = CacheBuilder.newBuilder()
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

  // The classes ReflectData.getClass loads for a writer schema: a java-class property, else
  // through arrays and unions with null, but not record fields, whose types come from the class
  void checkClassResolvedBy(Schema writerSchema) {
    switch (writerSchema.getType()) {
      case RECORD:
      case ENUM:
      case FIXED:
        check(writerSchema);
        break;
      case ARRAY:
        if (writerSchema.getProp(SpecificData.CLASS_PROP) != null) {
          checkClassProp(writerSchema, SpecificData.CLASS_PROP);
        } else {
          checkClassResolvedBy(writerSchema.getElementType());
        }
        break;
      case STRING:
        checkClassProp(writerSchema, SpecificData.CLASS_PROP);
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

  // Every class a reader schema from the registry names, by a named type or a java-class,
  // java-element-class or java-key-class property, since a reflect reader may load them all
  void checkClassesNamedBy(Schema readerSchema) {
    // The classes it names may resolve differently under another context class loader
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    CheckedFor checked = checkedSchemas.getIfPresent(readerSchema);
    if (checked == null || !checked.isFor(contextLoader)) {
      AvroSchemaUtils.forEachSchema(readerSchema, schema -> {
        switch (schema.getType()) {
          case RECORD:
          case ENUM:
          case FIXED:
            check(schema);
            break;
          case ARRAY:
            checkClassProp(schema, SpecificData.CLASS_PROP);
            checkClassProp(schema, SpecificData.ELEMENT_PROP);
            break;
          case STRING:
          case BYTES:
            checkClassProp(schema, SpecificData.CLASS_PROP);
            break;
          case MAP:
            checkClassProp(schema, SpecificData.KEY_CLASS_PROP);
            break;
          default:
            break;
        }
      });
      checkedSchemas.put(readerSchema, new CheckedFor(contextLoader));
    }
  }

  private void check(Schema schema) {
    // Found without initializing it
    checkTrusted(AvroSchemaUtils.findSpecificClass(schema), schema.getFullName());
  }

  private void checkClassProp(Schema schema, String prop) {
    String className = schema.getProp(prop);
    if (className != null) {
      checkTrusted(loadClass(className), className);
    }
  }

  private void checkTrusted(Class<?> clazz, String name) {
    if (allPackages) {
      // Every class is trusted, so there is nothing to check
      return;
    }
    if (clazz == null) {
      // Avro shares the classes it loads across the JVM, so it may still find one that another
      // context class loader exposed. A trusted name covers any class Avro finds by that name.
      if (configured && !isTrustedName(name)) {
        throw new SerializationException(name + " named by the schema cannot be found under the "
            + "current context class loader, so its trust for reflection deserialization cannot "
            + "be checked; add it to " + KafkaAvroDeserializerConfig.AVRO_TRUSTED_CLASSES_CONFIG
            + " or its package to " + KafkaAvroDeserializerConfig.AVRO_TRUSTED_PACKAGES_CONFIG
            + " to let Avro read it without a class, or trust every package with *");
      }
      return;
    }
    if (isTrusted(clazz)) {
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
    // As Avro checks an array class, by its component type
    while (clazz.isArray()) {
      clazz = clazz.getComponentType();
    }
    if (clazz.isPrimitive() || allPackages) {
      return true;
    }
    // Reading an annotation does not initialize the class
    if (classes.contains(clazz.getName()) || DEFAULT_CLASSES.contains(clazz.getName())
        || clazz.isAnnotationPresent(AvroGenerated.class)) {
      return true;
    }
    // A nested class reports its enclosing package
    Package classPackage = clazz.getPackage();
    return classPackage != null && isTrustedPackage(classPackage.getName());
  }

  // A class or schema name, such as com.example.Outer$Inner or com.example.Outer.Inner
  private boolean isTrustedName(String name) {
    if (classes.contains(name) || DEFAULT_CLASSES.contains(name)) {
      return true;
    }
    int dot = name.lastIndexOf('.');
    return dot > 0 && isTrustedPackage(name.substring(0, dot));
  }

  private boolean isTrustedPackage(String packageName) {
    for (String trusted : packages) {
      // "com.example" also trusts its subpackages, but not "com.examplex"
      if (packageName.startsWith(trusted) && (packageName.length() == trusted.length()
          || packageName.charAt(trusted.length()) == '.')) {
        return true;
      }
    }
    return false;
  }

  // ReflectData's lookup of a java-class property, without initializing the class: Avro's own
  // class loader, then the context class loader
  private static Class<?> loadClass(String className) {
    for (ClassLoader loader : new ClassLoader[] {ReflectData.class.getClassLoader(),
        Thread.currentThread().getContextClassLoader()}) {
      if (loader != null) {
        try {
          return Class.forName(className, false, loader);
        } catch (ClassNotFoundException e) {
          // try the next loader
        }
      }
    }
    return null;
  }

  // The context class loader a schema was checked under, held weakly
  private static final class CheckedFor {
    private final boolean hadContextLoader;
    private final WeakReference<ClassLoader> contextLoader;

    CheckedFor(ClassLoader contextLoader) {
      this.hadContextLoader = contextLoader != null;
      this.contextLoader = new WeakReference<>(contextLoader);
    }

    boolean isFor(ClassLoader currentLoader) {
      return hadContextLoader
          ? currentLoader != null && contextLoader.get() == currentLoader
          : currentLoader == null;
    }
  }
}
