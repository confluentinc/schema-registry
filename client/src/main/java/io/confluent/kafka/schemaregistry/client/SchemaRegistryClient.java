/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;

import com.google.common.base.Ticker;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public interface SchemaRegistryClient extends Closeable, SchemaVersionFetcher {

  default String tenant() {
    return DEFAULT_TENANT;
  }

  default Ticker ticker() {
    return Ticker.systemTicker();
  }

  Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references);

  default Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references,
      Metadata metadata,
      RuleSet ruleSet) {
    return parseSchema(schemaType, schemaString, references).map(s -> s.copy(metadata, ruleSet));
  }

  default Optional<ParsedSchema> parseSchema(Schema schema) {
    return parseSchema(
        schema.getSchemaType(),
        schema.getSchema(),
        schema.getReferences(),
        schema.getMetadata(),
        schema.getRuleSet()
    );
  }

  /**
   * @deprecated use {@link #register(String, ParsedSchema)} instead;
   *     for example, you can convert a {@link Schema} into a {@link ParsedSchema} 
   *     via {@code new AvroSchema(schema)}
   */
  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema) throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema));
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException;


  default int register(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated use {@link #register(String, ParsedSchema, int, int)} instead;
   *     for example, you can convert a {@link Schema} into a {@link ParsedSchema}
   *     via {@code new AvroSchema(schema)}
   */
  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema, int version, int id)
      throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema), version, id);
  }

  public int register(String subject, ParsedSchema schema, int version, int id) throws IOException,
      RestClientException;

  default RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    return registerWithResponse(subject, schema, normalize, false);
  }

  default RegisterSchemaResponse registerWithResponse(
      String subject, ParsedSchema schema, boolean normalize, boolean propagateSchemaTags)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /**
   * @deprecated use {@link #getSchemaById(int)} instead
   */
  @Deprecated
  default org.apache.avro.Schema getByID(int id) throws IOException, RestClientException {
    return getById(id);
  }

  /**
   * @deprecated use {@link #getSchemaById(int)} instead
   */
  @Deprecated
  default org.apache.avro.Schema getById(int id) throws IOException, RestClientException {
    ParsedSchema schema = getSchemaById(id);
    return schema instanceof AvroSchema ? ((AvroSchema) schema).rawSchema() : null;
  }

  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException;

  /**
   * @deprecated use {@link #getSchemaBySubjectAndId(String, int)} instead
   */
  @Deprecated
  default org.apache.avro.Schema getBySubjectAndID(String subject, int id)
      throws IOException, RestClientException {
    return getBySubjectAndId(subject, id);
  }

  /**
   * @deprecated use {@link #getSchemaBySubjectAndId(String, int)} instead
   */
  @Deprecated
  default org.apache.avro.Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    ParsedSchema schema = getSchemaBySubjectAndId(subject, id);
    return schema instanceof AvroSchema ? ((AvroSchema) schema).rawSchema() : null;
  }

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException;

  public default List<ParsedSchema> getSchemas(
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException;

  default Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
      RestClientException {
    throw new UnsupportedOperationException();
  }

  @Override
  default Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    throw new UnsupportedOperationException();
  }

  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException;

  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException;

  default SchemaMetadata getSchemaMetadata(String subject, int version,
      boolean lookupDeletedSchema) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  default SchemaMetadata getLatestWithMetadata(String subject, Map<String, String> metadata,
      boolean lookupDeletedSchema) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  default int getVersion(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getVersion(subject, new AvroSchema(schema));
  }

  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  default int getVersion(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public List<Integer> getAllVersions(String subject) throws IOException, RestClientException;

  default List<Integer> getAllVersions(String subject, boolean lookupDeletedSchema)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  default boolean testCompatibility(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return testCompatibility(subject, new AvroSchema(schema));
  }

  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  default List<String> testCompatibilityVerbose(String subject, ParsedSchema schema)
          throws IOException, RestClientException {
    return testCompatibilityVerbose(subject, schema, false);
  }

  default List<String> testCompatibilityVerbose(
      String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  default String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException {
    return updateConfig(subject, new Config(compatibility)).getCompatibilityLevel();
  }

  default String getCompatibility(String subject) throws IOException, RestClientException {
    return getConfig(subject).getCompatibilityLevel();
  }

  default void deleteCompatibility(String subject) throws IOException, RestClientException {
    deleteConfig(subject);
  }

  default Config updateConfig(String subject, Config config)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  default Config getConfig(String subject) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  default void deleteConfig(String subject) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public String setMode(String mode)
      throws IOException, RestClientException;

  public String setMode(String mode, String subject)
      throws IOException, RestClientException;

  default String setMode(String mode, String subject, boolean force)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public String getMode() throws IOException, RestClientException;

  public String getMode(String subject) throws IOException, RestClientException;

  default void deleteMode(String subject) throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public Collection<String> getAllSubjects() throws IOException, RestClientException;

  default Collection<String> getAllSubjects(boolean lookupDeletedSubject) throws IOException,
      RestClientException {
    throw new UnsupportedOperationException();
  }

  default Collection<String> getAllSubjectsByPrefix(String subjectPrefix) throws IOException,
      RestClientException {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  default int getId(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getId(subject, new AvroSchema(schema));
  }

  int getId(String subject, ParsedSchema schema) throws IOException, RestClientException;

  default int getId(String subject, ParsedSchema schema, boolean normalize)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public default List<Integer> deleteSubject(String subject) throws IOException,
          RestClientException {
    return deleteSubject(subject, false);
  }

  public default List<Integer> deleteSubject(String subject, boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public default List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException {
    return deleteSubject(requestProperties, subject, false);
  }

  public default List<Integer> deleteSubject(Map<String,
          String> requestProperties, String subject, boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public default Integer deleteSchemaVersion(String subject, String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(subject, version, false);
  }

  public default Integer deleteSchemaVersion(
          String subject,
          String version,
          boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public default Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(requestProperties, subject, version, false);
  }

  public default Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  public void reset();

  @Override
  default void close() throws IOException {}
}
