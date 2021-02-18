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

public interface SchemaRegistryClient extends SchemaVersionFetcher {

  /*
   * Takes a schema of AVRO, JSON, or PROTOBUF type and its references to form its ParsedSchema representation
   *
   * @return Optionally, ParsedSchema representation of the schema provided. Will return Optional.empty() if parsing is
   * not successful.
   */
  public Optional<ParsedSchema> parseSchema(
      String schemaType,
      String schemaString,
      List<SchemaReference> references);

  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema) throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema));
  }

  /*
   * Registers a ParsedSchema (the schema with references) to Schema Registry under the given subject
   *
   * @return The ID of the Schema registered
   */
  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException;

  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema, int version, int id)
      throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema), version, id);
  }

  /*
   * Registers a ParsedSchema (the schema with references) to Schema Registry under the given subject, version, and ID
   * NOTE: This only works if Schema Registry is in IMPORT mode.
   *
   * @return The ID of the Schema registered
   */
  public int register(String subject, ParsedSchema schema, int version, int id) throws IOException,
      RestClientException;

  @Deprecated
  default org.apache.avro.Schema getByID(int id) throws IOException, RestClientException {
    return getById(id);
  }

  @Deprecated
  default org.apache.avro.Schema getById(int id) throws IOException, RestClientException {
    ParsedSchema schema = getSchemaById(id);
    return schema instanceof AvroSchema ? ((AvroSchema) schema).rawSchema() : null;
  }

  /*
   * Fetches the ParsedSchema object that is represented by the given ID in Schema Registry.
   *
   * @return The ParsedSchema representation of the given ID
   */
  public ParsedSchema getSchemaById(int id) throws IOException, RestClientException;

  @Deprecated
  default org.apache.avro.Schema getBySubjectAndID(String subject, int id)
      throws IOException, RestClientException {
    return getBySubjectAndId(subject, id);
  }

  @Deprecated
  default org.apache.avro.Schema getBySubjectAndId(String subject, int id)
      throws IOException, RestClientException {
    ParsedSchema schema = getSchemaBySubjectAndId(subject, id);
    return schema instanceof AvroSchema ? ((AvroSchema) schema).rawSchema() : null;
  }

  /*
   * Fetches the ParsedSchema Object for a given Subject and ID
   *
   * @return The ParsedSchema Object
   */
  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException;

  /*
   * Fetches a list of Schemas that comply with the possible filters:
   *  - A subject prefix
   *  - Whether to include soft-deleted schemas
   *  - Whether to include all versions or just the latest registered version
   *
   * @return A list of schemas that comply with the filters
   */
  public default List<ParsedSchema> getSchemas(
      String subjectPrefix,
      boolean lookupDeletedSchema,
      boolean latestOnly)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Fetches the subjects that are associated to the given ID
   *
   * @return A Collection of subject names
   */
  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException;

  /*
   * Fetches SubjectVersion objects associated to a given ID
   *
   * @return A Collection of io.confluent.kafka.schemaregistry.client.rest.entities.SubjectVersion objects
   */
  default Collection<SubjectVersion> getAllVersionsById(int id) throws IOException,
      RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Fetches a Schema associated with the given subject and version.
   * Allows to filter for whether to include soft-deleted subject-versions in the lookup
   *
   * @return The io.confluent.kafka.schemaregistry.client.rest.entities.Schema object
   */
  @Override
  default Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    throw new UnsupportedOperationException();
  }

  /*
   * Fetches schema metadata associated to the latest version of the given subject.
   *
   * @return The io.confluent.kafka.schemaregistry.client.SchemaMetadata object
   */
  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException;

  /*
   * Fetches schema metadata associated to the given version of the given subject.
   *
   * @return The io.confluent.kafka.schemaregistry.client.SchemaMetadata object
   */
  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException;

  @Deprecated
  default int getVersion(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getVersion(subject, new AvroSchema(schema));
  }

  /*
   * Fetches the version number for the given ParsedSchema under the given subject (if it exists)
   *
   * @return The version number
   */
  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  /*
   * Fetches all versions for a given subject
   *
   * @return A List of versions under the subject
   */
  public List<Integer> getAllVersions(String subject) throws IOException, RestClientException;

  @Deprecated
  default boolean testCompatibility(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return testCompatibility(subject, new AvroSchema(schema));
  }

  /*
   * Performs a compatibility check between the given ParsedSchema and the compatibility requirements of the
   * given subject.
   *
   * @return true if it is compatible according to subject rules
   */
  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  /*
   * Performs a compatibility check between the given ParsedSchema and the compatibility requirements of the
   * given subject. It returns all messages from the compatibility request.
   *
   * @return A List of messages returned by the compatibility check
   */
  default List<String> testCompatibilityVerbose(String subject, ParsedSchema schema)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Updates the compatibility for a given subject
   *
   * @return The compatibility of the subject after the update
   */
  public String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException;

  /*
   * Fetches the compatibility for a given subject
   *
   * @return The compatibility of the subject
   */
  public String getCompatibility(String subject) throws IOException, RestClientException;

  /*
   * Sets the cluster-wide mode of the Schema Registry cluster
   *
   * @return The mode of the cluster after performing the set
   */
  public String setMode(String mode)
      throws IOException, RestClientException;

  /*
   * Sets the mode of the subject in the Schema Registry cluster
   *
   * @return The mode of the subject after performing the set
   */
  public String setMode(String mode, String subject)
      throws IOException, RestClientException;

  /*
   * Fetches the current mode of the Schema Registry cluster
   *
   * @return The mode of the cluster
   */
  public String getMode() throws IOException, RestClientException;

  /*
   * Fetches the current mode of the given subject
   *
   * @return The mode of the subject
   */
  public String getMode(String subject) throws IOException, RestClientException;

  /*
   * Fetches a list of all available subjects in the Schema Registry Cluster
   *
   * @return A Collection of all subjects from the cluster
   */
  public Collection<String> getAllSubjects() throws IOException, RestClientException;

  @Deprecated
  default int getId(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getId(subject, new AvroSchema(schema));
  }

  /*
   * Fetches the ID of a ParsedSchema in the Schema Registry under the given subject
   *
   * @return The ID of the schema
   */
  int getId(String subject, ParsedSchema schema) throws IOException, RestClientException;

  /*
   * Soft deletes an entire subject in the Schema Registry
   *
   * @return A list of the versions deleted
   */
  public default List<Integer> deleteSubject(String subject) throws IOException,
          RestClientException {
    return deleteSubject(subject, false);
  }

  /*
   * Deletes an entire subject in the Schema Registry, optionally performing a 'Hard' or 'Permanent' deletion
   *
   * @return A list of the versions deleted
   */
  public default List<Integer> deleteSubject(String subject, boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Deletes an entire subject in the Schema Registry, allowing for extra request properties to be passed
   *
   * @return A list of the versions deleted
   */
  public default List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException {
    return deleteSubject(requestProperties, subject, false);
  }

  /*
   * Deletes an entire subject in the Schema Registry, optionally performing a 'Hard' or 'Permanent' deletion
   * Also allows for extra request properties to be passed
   *
   * @return A list of the versions deleted
   */
  public default List<Integer> deleteSubject(Map<String,
          String> requestProperties, String subject, boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Soft Deletes a specific version from a subject in the Schema Registry
   *
   * @return The version number that was deleted
   */
  public default Integer deleteSchemaVersion(String subject, String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(subject, version, false);
  }

  /*
   * Soft Deletes a specific version from a subject in the Schema Registry, optionally performing a 'Hard' or 'Permanent' deletion
   *
   * @return The version number that was deleted
   */
  public default Integer deleteSchemaVersion(
          String subject,
          String version,
          boolean isPermanent)
          throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Soft Deletes a specific version from a subject in the Schema Registry
   * Also allows for extra request properties to be passed
   *
   * @return The version number that was deleted
   */
  public default Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version)
      throws IOException, RestClientException {
    return deleteSchemaVersion(requestProperties, subject, version, false);
  }

  /*
   * Deletes a specific version from a subject in the Schema Registry, optionally performing a 'Hard' or 'Permanent' deletion
   * Also allows for extra request properties to be passed
   *
   * @return The version number that was deleted
   */
  public default Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version,
      boolean isPermanent)
      throws IOException, RestClientException {
    throw new UnsupportedOperationException();
  }

  /*
   * Resets the SchemaRegistryClient to initial state.
   *
   * @return Nothing
   */
  public void reset();
}
