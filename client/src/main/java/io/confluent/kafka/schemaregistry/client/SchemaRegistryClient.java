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

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public interface SchemaRegistryClient extends SchemaVersionFetcher {

  Map<String, SchemaProvider> getSchemaProviders();

  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema) throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema));
  }

  public int register(String subject, ParsedSchema schema) throws IOException, RestClientException;

  @Deprecated
  default int register(String subject, org.apache.avro.Schema schema, int version, int id)
      throws IOException,
      RestClientException {
    return register(subject, new AvroSchema(schema), version, id);
  }

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

  public ParsedSchema getSchemaBySubjectAndId(String subject, int id)
      throws IOException, RestClientException;

  public Collection<String> getAllSubjectsById(int id) throws IOException, RestClientException;

  @Override
  default Schema getByVersion(String subject, int version, boolean lookupDeletedSchema) {
    throw new UnsupportedOperationException();
  }

  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException;

  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException;

  @Deprecated
  default int getVersion(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getVersion(subject, new AvroSchema(schema));
  }

  public int getVersion(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  public List<Integer> getAllVersions(String subject) throws IOException, RestClientException;

  @Deprecated
  default boolean testCompatibility(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return testCompatibility(subject, new AvroSchema(schema));
  }

  public boolean testCompatibility(String subject, ParsedSchema schema)
      throws IOException, RestClientException;

  public String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException;

  public String getCompatibility(String subject) throws IOException, RestClientException;

  public String setMode(String mode)
      throws IOException, RestClientException;

  public String setMode(String mode, String subject)
      throws IOException, RestClientException;

  public String getMode() throws IOException, RestClientException;

  public String getMode(String subject) throws IOException, RestClientException;

  public Collection<String> getAllSubjects() throws IOException, RestClientException;

  @Deprecated
  default int getId(String subject, org.apache.avro.Schema schema)
      throws IOException, RestClientException {
    return getId(subject, new AvroSchema(schema));
  }

  int getId(String subject, ParsedSchema schema) throws IOException, RestClientException;

  public List<Integer> deleteSubject(String subject) throws IOException, RestClientException;

  public List<Integer> deleteSubject(Map<String, String> requestProperties, String subject)
      throws IOException, RestClientException;

  public Integer deleteSchemaVersion(String subject, String version)
      throws IOException, RestClientException;

  public Integer deleteSchemaVersion(
      Map<String, String> requestProperties,
      String subject,
      String version)
      throws IOException, RestClientException;

  public void reset();
}
