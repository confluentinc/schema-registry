/**
 * Copyright 2014 Confluent Inc.
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

import org.apache.avro.Schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public interface SchemaRegistryClient {

  public int register(String subject, Schema schema) throws IOException, RestClientException;

  public int register(String subject, Schema schema, int id) throws IOException,
      RestClientException;

  @Deprecated
  public Schema getByID(int id) throws IOException, RestClientException;

  public Schema getById(int id) throws IOException, RestClientException;

  @Deprecated
  public Schema getBySubjectAndID(String subject, int id) throws IOException, RestClientException;

  public Schema getBySubjectAndId(String subject, int id) throws IOException, RestClientException;

  public SchemaMetadata getLatestSchemaMetadata(String subject)
      throws IOException, RestClientException;

  public SchemaMetadata getSchemaMetadata(String subject, int version)
      throws IOException, RestClientException;

  public int getVersion(String subject, Schema schema) throws IOException, RestClientException;

  public List<Integer> getAllVersions(String subject) throws IOException, RestClientException;

  public boolean testCompatibility(String subject, Schema schema)
      throws IOException, RestClientException;

  public String updateCompatibility(String subject, String compatibility)
      throws IOException, RestClientException;

  public String getCompatibility(String subject) throws IOException, RestClientException;

  public String setMode(String mode)
      throws IOException, RestClientException;

  public String getMode() throws IOException, RestClientException;

  public Collection<String> getAllSubjects() throws IOException, RestClientException;

  int getId(String subject, Schema schema) throws IOException, RestClientException;

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
}
