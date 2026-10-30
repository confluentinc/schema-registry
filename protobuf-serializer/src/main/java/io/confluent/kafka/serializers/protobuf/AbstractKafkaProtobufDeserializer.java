/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.serializers.protobuf;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaAndValue;
import io.confluent.kafka.schemaregistry.rules.RuleResult;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.rules.RulePhase;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import io.confluent.kafka.serializers.provenance.ProvenanceProjector;
import io.confluent.kafka.serializers.provenance.ProvenanceReaderMarks;
import io.confluent.kafka.serializers.provenance.ProvenanceUnavailableException;
import io.confluent.kafka.serializers.provenance.ReaderSchema;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import java.io.InterruptedIOException;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.MessageIndexes;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;

public abstract class AbstractKafkaProtobufDeserializer<T extends Message>
    extends AbstractKafkaSchemaSerDe {

  private static final int DEFAULT_CACHE_CAPACITY = 1000;

  protected Class<T> specificProtobufClass;
  protected Method parseMethod;
  protected volatile boolean deriveType;
  private final Cache<Pair<String, ProtobufSchema>, ProtobufSchema> schemaCache;

  public AbstractKafkaProtobufDeserializer() {
    schemaCache = CacheBuilder.newBuilder()
        .maximumSize(DEFAULT_CACHE_CAPACITY)
        .build();
  }

  /**
   * Sets properties for this deserializer without overriding the schema registry client itself.
   * Useful for testing, where a mock client is injected.
   */
  protected void configure(KafkaProtobufDeserializerConfig config, Class<T> type) {
    configureClientProperties(config, new ProtobufSchemaProvider());
    try {
      this.specificProtobufClass = type;
      if (specificProtobufClass != null && !specificProtobufClass.equals(Object.class)) {
        this.parseMethod = specificProtobufClass.getDeclaredMethod(
            "parseFrom", ByteBuffer.class, ExtensionRegistryLite.class);
      }
      this.deriveType = config.getBoolean(KafkaProtobufDeserializerConfig.DERIVE_TYPE_CONFIG);
    } catch (Exception e) {
      throw new ConfigException("Class " + specificProtobufClass.getCanonicalName()
          + " is not a valid protobuf message class", e);
    }
    // Last, once every flag a projection and its reader are built under is set: one built by a read
    // in between would keep the old ones.
    resetProvenance();
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(Map<String, ?> props) {
    try {
      return new KafkaProtobufDeserializerConfig(props);
    } catch (ConfigException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  protected KafkaProtobufDeserializerConfig deserializerConfig(Properties props) {
    return new KafkaProtobufDeserializerConfig(props);
  }

  /**
   * Deserializes the payload without including schema information for primitive types, maps, and
   * arrays. Just the resulting deserialized object is returned.
   *
   * <p>This behavior is the norm for Decoders/Deserializers.
   *
   * @param payload serialized data
   * @return the deserialized object
   */
  protected T deserialize(byte[] payload)
      throws SerializationException, InvalidConfigurationException {
    return (T) deserialize(false, null, isKey, payload);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(includeSchemaAndVersion, topic, isKey, null, payload);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(includeSchemaAndVersion, topic, isKey, headers, payload, null);
  }

  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean key, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc
  ) throws SerializationException, InvalidConfigurationException {
    return deserialize(
        includeSchemaAndVersion, topic, key, headers, payload, writerToReaderSchemaFunc, false);
  }

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected Object deserialize(
      boolean includeSchemaAndVersion, String topic, Boolean key, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc,
      boolean includeRuleResults
  ) throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException(
          "SchemaRegistryClient not found. You need to configure the deserializer "
              + "or use deserializer constructor with SchemaRegistryClient.");
    }
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null;
    }

    boolean isKey = key != null ? key : this.isKey;
    SchemaId schemaId = new SchemaId(ProtobufSchema.TYPE);
    List<RuleResult> ruleResults = includeRuleResults ? new ArrayList<>() : null;
    try (SchemaIdDeserializer schemaIdDeserializer = schemaIdDeserializer(isKey)) {
      ByteBuffer buffer =
          schemaIdDeserializer.deserialize(topic, isKey, headers, payload, schemaId);
      String subject = strategyUsesSchema(isKey)
          ? getContextName(topic) : subjectName(topic, isKey, null);
      ProtobufSchema schema = (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
      MessageIndexes indexes = new MessageIndexes(schemaId.getMessageIndexes());
      String name = schema.toMessageName(indexes);
      schema = schemaWithName(schema, name);
      if (subject == null || strategyUsesSchema(isKey)) {
        subject = subjectName(topic, isKey, schema);
        // Fetched again by id it comes back unnamed, as its file's first message: name it again.
        schema = schemaWithName(schemaForDeserialize(schemaId, schema, subject, isKey), name);
      }
      Object buf = executeRules(
          subject, topic, headers, payload, RulePhase.ENCODING, RuleMode.READ, null,
          schema, buffer, ruleResults
      );
      buffer = buf instanceof byte[] ? ByteBuffer.wrap((byte[]) buf) : (ByteBuffer) buf;

      List<Migration> migrations = Collections.emptyList();
      ProtobufSchema readerSchema = writerToReaderSchemaFunc != null
          ? (ProtobufSchema) writerToReaderSchemaFunc.apply(schema)
          : null;
      if (readerSchema == null) {
        if (metadata != null) {
          readerSchema = (ProtobufSchema) getLatestWithMetadata(subject).getSchema();
        } else if (useLatestVersion) {
          readerSchema = (ProtobufSchema) lookupLatestVersion(subject, schema, false).getSchema();
        }
        if (readerSchema != null && readerSchema.toDescriptor(name) != null) {
          readerSchema = schemaWithName(readerSchema, name);
        }
        if (includeSchemaAndVersion || readerSchema != null) {
          Integer version = schemaVersion(topic, isKey, schemaId, subject, schema, null);
          schema = schema.copy(version);
          schema = schemaWithName(schema, name);
        }
        if (readerSchema != null) {
          migrations = getMigrations(subject, schema, readerSchema);
        }
      } else if (readerSchema.toDescriptor(name) != null) {
        readerSchema = namedReader(readerSchema, name);
      }
      // With no reader configured, a generated class's schema is the reader.
      ProtobufSchema provenanceReader = readerSchema != null ? readerSchema : classSchema(schema);
      ProtoProvenanceProjection projection =
          byProvenance(subject, schemaId, schema, provenanceReader, name, migrations);

      int length = buffer.remaining();
      int start = buffer.position() + buffer.arrayOffset();

      Object message = null;
      if (!migrations.isEmpty()) {
        message = DynamicMessage.parseFrom(schema.toDescriptor(),
            CodedInputStream.newInstance(buffer.array(), start, length),
            ProtobufSchema.EXTENSION_REGISTRY);
        message = executeMigrations(migrations, subject, topic, headers, message);
        message = readerSchema.fromJson((JsonNode) message);
      } else if (parsesProjected(projection, readerSchema, schema)) {
        message = parseProjected(projection, provenanceReader, buffer, start, length);
      }

      ProtobufSchema writerSchema = schema;
      if (readerSchema != null) {
        schema = readerSchema;
      }
      if (schema.ruleSet() != null && schema.ruleSet().hasRules(RulePhase.DOMAIN, RuleMode.READ)) {
        if (message == null) {
          message = DynamicMessage.parseFrom(schema.toDescriptor(),
              CodedInputStream.newInstance(buffer.array(), start, length),
              ProtobufSchema.EXTENSION_REGISTRY);
        }
        message = projectedRuled(projection, readerSchema, provenanceReader, executeRules(
            subject, topic, headers, payload, RulePhase.DOMAIN, RuleMode.READ, null,
            schema, message, ruleResults
        ));
      }

      boolean parsed = parseMethod == null && !deriveType && isParsed(message, schema);
      ByteBuffer protobufBytes = buffer;
      if (message != null && !parsed) {
        protobufBytes = ByteBuffer.wrap(((Message) message).toByteArray());
        length = protobufBytes.limit();
        start = 0;
      }

      Object value;
      if (parseMethod != null) {
        try {
          value = parseMethod.invoke(null, protobufBytes, ProtobufSchema.EXTENSION_REGISTRY);
        } catch (Exception e) {
          throw new ConfigException("Not a valid protobuf builder", e);
        }
      } else if (deriveType) {
        value = deriveType(protobufBytes, schema);
      } else {
        value = parsed ? message : parseDynamic(schema, protobufBytes, start, length);
      }

      if (includeSchemaAndVersion) {
        // Annotate the schema with the version. Note that we only do this if the schema +
        // version are requested, i.e. in Kafka Connect converters. This is critical because that
        // code *will not* rely on exact schema equality. Regular deserializers *must not* include
        // this information because it would return schemas which are not equivalent.
        //
        // Note, however, that we also do not fill in the connect.version field. This allows the
        // Converter to let a version provided by a Kafka Connect source take priority over the
        // schema registry's ordering (which is implicit by auto-registration time rather than
        // explicit from the Connector).

        Integer writerVersion = schemaVersion(topic, isKey, schemaId, subject, writerSchema, null);
        ParsedSchemaAndValue.SchemaInfo writerInfo = new ParsedSchemaAndValue.SchemaInfo(
            subject,
            schemaId.getId(),
            writerVersion,
            schemaId.getGuid());
        List<RuleResult> ruleResultsCopy = ruleResults == null || ruleResults.isEmpty()
            ? Collections.emptyList()
            : Collections.unmodifiableList(new ArrayList<>(ruleResults));
        return new ProtobufSchemaAndValue(schema, value, writerInfo, writerSchema, ruleResultsCopy);
      }

      return value;
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error deserializing Protobuf message for id " + schemaId, e);
    } catch (IOException | RuntimeException e) {
      throw toDeserializationException(
          e, "Error deserializing Protobuf message for id " + schemaId);
    } catch (RestClientException e) {
      throw toKafkaException(e, "Error retrieving Protobuf schema for id " + schemaId);
    } finally {
      postOp(payload);
    }
  }

  /**
   * Whether {@code message} is already what a dynamic read into {@code schema} returns, so needs no
   * second parse: in its own descriptor, and with no required field a rule left unset, which the
   * parse would reject.
   */
  private static boolean isParsed(Object message, ProtobufSchema schema) {
    return message instanceof DynamicMessage
        && ((Message) message).getDescriptorForType() == schema.toDescriptor()
        && ((Message) message).isInitialized();
  }

  /**
   * With provenance, the schema of the generated class a specific or derived read parses into;
   * null for a dynamic read, or with no class to be found.
   */
  private ProtobufSchema classSchema(ProtobufSchema writer) {
    if (!provenanceEnabled || (parseMethod == null && !deriveType)) {
      return null;
    }
    String name = parseMethod != null ? specificProtobufClass.getName() : writer.fullName();
    if (name == null) {
      return null;
    }
    // The projector first: a load holding the map's lock must not wait for the monitor a reset,
    // clearing the map, holds.
    ProvenanceProjector<ProtoProvenanceProjection> projector = provenanceProjector();
    return classSchemas.computeIfAbsent(name,
        n -> Optional.ofNullable(loadClassSchema(n, projector))).orElse(null);
  }

  private ProtobufSchema loadClassSchema(String name,
      ProvenanceProjector<ProtoProvenanceProjection> projector) {
    try {
      // As deriveType resolves it: not initialized until it is known to be a message class.
      Class<?> cls = parseMethod != null ? specificProtobufClass : Class.forName(
          name, false, AbstractKafkaProtobufDeserializer.class.getClassLoader());
      if (!Message.class.isAssignableFrom(cls)) {
        return null;
      }
      Message instance = (Message) cls.getMethod("getDefaultInstance").invoke(null);
      return (ProtobufSchema) projector.derivedReader(
          new ProtobufSchema(instance.getDescriptorForType()));
    } catch (ReflectiveOperationException e) {
      return null;
    }
  }

  // Built once per class, by name: a class's schema never changes, nor does a class not there.
  private final Map<String, Optional<ProtobufSchema>> classSchemas = new ConcurrentHashMap<>();

  /**
   * Whether a projected read is parsed before the domain rules: it is, except for the writer's
   * own rules with no reader configured, which read the writer's record; what the class does not
   * pair with it is dropped after them.
   */
  private static boolean parsesProjected(ProtoProvenanceProjection projection,
      ProtobufSchema reader, ProtobufSchema writer) {
    return projection != null && projection.movedAny() && (reader != null || !hasReadRules(writer));
  }

  /**
   * {@code ruled} in the class's own numbers, when the writer's rules ran in the writer's: what
   * they wrote under a moved number is the writer's field, which the class does not have.
   */
  private static Object projectedRuled(ProtoProvenanceProjection projection,
      ProtobufSchema reader, ProtobufSchema provenanceReader, Object ruled) throws IOException {
    if (reader != null || projection == null || !projection.movedAny()) {
      return ruled;
    }
    byte[] bytes = ((Message) ruled).toByteArray();
    return parseProjected(projection, provenanceReader, ByteBuffer.wrap(bytes), 0, bytes.length);
  }

  private static boolean hasReadRules(ProtobufSchema schema) {
    return schema.ruleSet() != null && schema.ruleSet().hasRules(RulePhase.DOMAIN, RuleMode.READ);
  }

  // A copy naming the written message: a supplied id stands for it too.
  private ProtobufSchema namedReader(ProtobufSchema reader, String name) {
    ProtobufSchema named = schemaWithName(reader, name);
    return !provenanceEnabled ? named
        : (ProtobufSchema) provenanceProjector().sameReader(reader, named);
  }

  private static Message parseDynamic(ProtobufSchema schema, ByteBuffer bytes, int start,
      int length) throws IOException {
    return DynamicMessage.parseFrom(descriptorOf(schema),
        CodedInputStream.newInstance(bytes.array(), start, length),
        ProtobufSchema.EXTENSION_REGISTRY);
  }

  private static Descriptor descriptorOf(ProtobufSchema schema) {
    Descriptor descriptor = schema.toDescriptor();
    if (descriptor == null) {
      throw new SerializationException("Could not find descriptor with name " + schema.name());
    }
    return descriptor;
  }

  /**
   * {@code bytes} parsed as {@code reader} with its new fields unset, and none of the data under
   * their numbers. Done before the domain rules: a value they write to a new field must land under
   * its own number, and a caller handed the parse descriptor could not address the fields it
   * leaves out. Package-private for tests.
   */
  static Message parseProjected(ProtoProvenanceProjection projection,
      ProtobufSchema reader, ByteBuffer bytes, int start, int length) throws IOException {
    // The reader's own parse less the new fields and the unknown data under their numbers, save
    // where a new oneof member may have displaced a kept one.
    DynamicMessage.Builder direct = DynamicMessage.newBuilder(descriptorOf(reader));
    boolean parsed = false;
    try {
      direct.mergeFrom(CodedInputStream.newInstance(bytes.array(), start, length),
          ProtobufSchema.EXTENSION_REGISTRY);
      parsed = true;
    } catch (IOException | RuntimeException e) {
      // As a new field's type may not parse what lies under its number.
    }
    // Outside the catch: a failure of the clearing itself fails the record rather than hiding.
    if (parsed && projection.clearMoved(direct) && direct.isInitialized()) {
      return direct.buildPartial();
    }
    // Else parsed with the reader less its new fields, and back in the reader's own descriptor.
    Message pruned = parseDynamic(projection.schema(), bytes, start, length);
    return DynamicMessage.parseFrom(reader.toDescriptor(),
        projection.dropMoved(pruned).toByteString(), ProtobufSchema.EXTENSION_REGISTRY);
  }

  private ProtoProvenanceProjection byProvenance(String subject, SchemaId writerId,
      ProtobufSchema writer, ProtobufSchema reader, String name, List<Migration> migrations) {
    if (!provenanceEnabled || reader == null || !migrations.isEmpty()) {
      return null;
    }
    if (!name.equals(rootOf(reader)) && reader.toDescriptor(name) == null) {
      // Read without provenance, the record would be parsed as the reader's first message.
      throw new SerializationException("The record was written as message " + name
          + ", which the reader schema does not declare");
    }
    // The writer named at the record's message: it may come back unnamed, as its file's first,
    // and its name tells each record's message apart, here and in the projector's cache.
    ProtobufSchema named = name.equals(writer.name()) ? writer : schemaWithName(writer, name);
    ProvenanceProjector<ProtoProvenanceProjection> projector = provenanceProjector();
    return projector.project(subject, writerId, named, reader,
        mapping -> {
          // Each side's locations are its version's, read as that version's schema names: its
          // one message, or every top-level message when it names no root and declares several.
          // A reader or writer matched to the version by structure need not be read so.
          ProtobufSchema writerVersion = version(projector, subject, mapping.writerId());
          ProtobufSchema readerVersion = version(projector, subject, mapping.readerId());
          boolean writerMulti = writerVersion.readsAsSeveralMessages();
          boolean readerMulti = readerVersion.readsAsSeveralMessages();
          // A version read as one message has locations for that message only: a record written
          // as another, or read by a reader whose version is rooted at another, has none.
          if (!(writerMulti || name.equals(rootOf(writerVersion)))
              || !(readerMulti || name.equals(rootOf(readerVersion)))) {
            throw new ProvenanceUnavailableException("The record was written as message " + name
                + ", not the root of a schema read as one message, which provenance has no "
                + "locations for");
          }
          return ProtoProvenanceProjection.of(reader, readerMulti, named, writerMulti, mapping);
        }).orElse(null);
  }

  private static ProtobufSchema version(ProvenanceProjector<ProtoProvenanceProjection> projector,
      String subject, int id) {
    ParsedSchema schema = projector.registered(subject, id);
    if (!(schema instanceof ProtobufSchema)) {
      throw new ProvenanceUnavailableException("Schema id " + id + " of subject " + subject
          + " is not a Protobuf schema");
    }
    return (ProtobufSchema) schema;
  }

  // The message a schema read as one message is rooted at: the one its root param names, or its
  // file's first; a name the schema was given is not one.
  private static String rootOf(ProtobufSchema schema) {
    String root = schema.rootParam();
    if (root != null) {
      return root;
    }
    ProtoFileElement file = schema.effectiveFile();
    for (TypeElement type : file.getTypes()) {
      if (type instanceof MessageElement) {
        String packageName = file.getPackageName();
        return packageName == null || packageName.isEmpty()
            ? type.getName() : packageName + "." + type.getName();
      }
    }
    return null;
  }

  private volatile ProvenanceProjector<ProtoProvenanceProjection> provenanceProjector;
  // Which readers were pinned or derived: kept for the deserializer's life, as a reset during a
  // read must not unmark its reader.
  private final ProvenanceReaderMarks provenanceReaderMarks = new ProvenanceReaderMarks();

  /**
   * {@code readers} as a reader function, with any registered id a reader comes with used for
   * provenance instead of being looked up.
   */
  protected Function<ParsedSchema, ParsedSchema> readerSchemas(
      Function<ParsedSchema, ReaderSchema> readers) {
    // Without provenance a pin means nothing: read with the caller's schema as given.
    if (!provenanceEnabled) {
      return writer -> {
        ReaderSchema reader = readers.apply(writer);
        return reader != null ? reader.getSchema() : null;
      };
    }
    return provenanceProjector().readerSchemas(readers);
  }

  /**
   * Forgets the projector, and the class schemas it marked: they belong to the configuration they
   * were built under. Under the lock the projector is built under.
   */
  private synchronized void resetProvenance() {
    provenanceProjector = null;
    classSchemas.clear();
  }

  @Override
  protected boolean readsByProvenance() {
    return true;
  }

  // Created on first use, once the deserializer is configured, and only once: it holds the ids
  // readers were supplied with and which readers a class derived.
  private ProvenanceProjector<ProtoProvenanceProjection> provenanceProjector() {
    ProvenanceProjector<ProtoProvenanceProjection> projector = provenanceProjector;
    if (projector == null) {
      synchronized (this) {
        projector = provenanceProjector;
        if (projector == null) {
          projector = new ProvenanceProjector<>(schemaRegistry, provenanceCacheSize,
              provenanceCacheTtlSec, provenanceStrategy, provenanceReaderMarks);
          provenanceProjector = projector;
        }
      }
    }
    return projector;
  }

  private ProtobufSchema schemaWithName(ProtobufSchema schema, String name) {
    Pair<String, ProtobufSchema> cacheKey = new Pair<>(name, schema);
    try {
      return schemaCache.get(cacheKey, () -> schema.copy(name));
    } catch (ExecutionException e) {
      return schema.copy(name);
    }
  }

  private Object deriveType(ByteBuffer buffer, ProtobufSchema schema) {
    String clsName = schema.fullName();
    if (clsName == null) {
      throw new SerializationException("If `derive.type` is true, then either "
          + "`java_outer_classname` or `java_multiple_files = true` must be set "
          + "in the Protobuf schema");
    }
    try {
      // resolve without initializing so no static initializer runs before the type check
      Class<?> cls = Class.forName(
          clsName, false, AbstractKafkaProtobufDeserializer.class.getClassLoader());
      if (!Message.class.isAssignableFrom(cls)) {
        throw new SerializationException("Class " + clsName
            + " is not a valid protobuf message class");
      }
      Method parseMethod = cls.getDeclaredMethod(
          "parseFrom", ByteBuffer.class, ExtensionRegistryLite.class);
      return parseMethod.invoke(null, buffer, ProtobufSchema.EXTENSION_REGISTRY);
    } catch (ClassNotFoundException e) {
      throw new SerializationException("Class " + clsName + " could not be found.");
    } catch (NoSuchMethodException e) {
      throw new SerializationException("Class " + clsName
          + " is not a valid protobuf message class", e);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new SerializationException("Not a valid protobuf builder");
    }
  }

  private Integer schemaVersion(
      String topic, boolean isKey, SchemaId schemaId,
      String subject, ProtobufSchema schema, Object value
  ) throws IOException, RestClientException {
    Integer version = null;
    ProtobufSchema subjectSchema = (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
    Metadata metadata = subjectSchema.metadata();
    if (metadata != null) {
      version = metadata.getConfluentVersionNumber();
    }
    if (version == null) {
      version = schemaRegistry.getVersion(subject, subjectSchema);
    }
    return version;
  }

  private String subjectName(String topic, boolean isKey, ProtobufSchema schemaFromRegistry) {
    return getSubjectName(topic, isKey, null, schemaFromRegistry);
  }

  private ProtobufSchema schemaForDeserialize(
      SchemaId schemaId, ProtobufSchema schemaFromRegistry, String subject, boolean isKey
  ) throws IOException, RestClientException {
    return (ProtobufSchema) getSchemaBySchemaId(subject, schemaId);
  }

  protected ProtobufSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload
  ) throws SerializationException {
    return (ProtobufSchemaAndValue) deserialize(true, topic, isKey, headers, payload);
  }

  protected ProtobufSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc
  ) throws SerializationException {
    return deserializeWithSchemaAndVersion(
        topic, isKey, headers, payload, writerToReaderSchemaFunc, false);
  }

  protected ProtobufSchemaAndValue deserializeWithSchemaAndVersion(
      String topic, boolean isKey, Headers headers, byte[] payload,
      Function<ParsedSchema, ParsedSchema> writerToReaderSchemaFunc,
      boolean includeRuleResults
  ) throws SerializationException {
    return (ProtobufSchemaAndValue) deserialize(
        true, topic, isKey, headers, payload, writerToReaderSchemaFunc, includeRuleResults);
  }

  static class Pair<K, V> {
    private final K key;
    private final V value;

    public Pair(K key, V value) {
      this.key = key;
      this.value = value;
    }

    public K getKey() {
      return key;
    }

    public V getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Pair<?, ?> pair = (Pair<?, ?>) o;
      return Objects.equals(key, pair.key)
          && Objects.equals(value, pair.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value);
    }

    @Override
    public String toString() {
      return "Pair{"
          + "key=" + key
          + ", value=" + value
          + '}';
    }
  }
}
