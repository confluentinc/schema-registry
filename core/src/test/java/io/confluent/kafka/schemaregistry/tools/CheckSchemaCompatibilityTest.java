/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.tools;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for CheckSchemaCompatibility tool.
 */
public class CheckSchemaCompatibilityTest {

  @Mock
  private SchemaRegistryClient sourceClient;

  @Mock
  private SchemaRegistryClient targetClient;

  private ParsedSchema parsedSchema1;
  private ParsedSchema parsedSchema2;

  private CheckSchemaCompatibility tool;

  private static final String SUBJECT_NAME = "test-subject";
  private static final String SCHEMA_STRING_1 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";
  private static final String SCHEMA_STRING_2 = "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    tool = new CheckSchemaCompatibility();
    
    // Create real ParsedSchema objects using AvroSchema
    parsedSchema1 = new AvroSchema(SCHEMA_STRING_1);
    parsedSchema2 = new AvroSchema(SCHEMA_STRING_2);
  }

  @Test
  void testCompareSubjects_PerfectMatch() throws Exception {
    // Setup
    List<String> sourceSubjects = Arrays.asList("subject1", "subject2");
    List<String> targetSubjects = Arrays.asList("subject1", "subject2");

    // Mock successful subject comparison
    setupSuccessfulSubjectComparison("subject1");
    setupSuccessfulSubjectComparison("subject2");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareSubjects_DifferentSubjectCounts() throws Exception {
    // Setup
    List<String> sourceSubjects = Arrays.asList("subject1", "subject2");
    List<String> targetSubjects = Arrays.asList("subject1");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubjects_EmptyRegistries() throws Exception {
    // Setup
    List<String> sourceSubjects = Collections.emptyList();
    List<String> targetSubjects = Collections.emptyList();

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareSubjects_OnlySourceEmpty() throws Exception {
    // Setup
    List<String> sourceSubjects = Collections.emptyList();
    List<String> targetSubjects = Arrays.asList("subject1");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareSubjects_SubjectComparisonFails() throws Exception {
    // Setup
    List<String> sourceSubjects = Arrays.asList("subject1");
    List<String> targetSubjects = Arrays.asList("subject1");

    // Mock failed subject comparison
    setupFailedSubjectComparison("subject1");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_IdenticalSchemas() throws Exception {
    // Setup
    setupIdenticalSchemas();

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareSubject_DifferentVersionCounts() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_DifferentVersionNumbers() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 3));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_SchemaMismatch() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_2);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects with different schemas - they should NOT be equivalent
    ParsedSchema schema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2 = new AvroSchema(SCHEMA_STRING_2);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_VersionNumberMismatch() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(2, 100, SCHEMA_STRING_1); // Different version

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Mock parsed schemas that are equivalent
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_ParseFailure() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Mock parse failure
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.empty());
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_ExceptionHandling() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenThrow(new RestClientException("Connection failed", 500, 50001));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareSubject_MultipleVersionsSuccess() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));

    // Setup for version 1
    SchemaMetadata sourceMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 200, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    // Setup for version 2
    SchemaMetadata sourceMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_2);
    SchemaMetadata targetMetadata2 = createSchemaMetadata(2, 201, SCHEMA_STRING_2);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(sourceMetadata2);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(targetMetadata2);

    // Use real ParsedSchema objects - all schemas are the same, so they should be equivalent
    ParsedSchema schema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2 = new AvroSchema(SCHEMA_STRING_2);
    when(sourceClient.parseSchema(any(Schema.class)))
        .thenReturn(Optional.of(schema1))
        .thenReturn(Optional.of(schema2));
    when(targetClient.parseSchema(any(Schema.class)))
        .thenReturn(Optional.of(schema1))
        .thenReturn(Optional.of(schema2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareSubject_MultipleVersionsOneFails() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));

    // Setup for version 1 - success
    SchemaMetadata sourceMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 200, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    // Setup for version 2 - failure (different schemas)
    SchemaMetadata sourceMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata2 = createSchemaMetadata(2, 201, SCHEMA_STRING_2);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(sourceMetadata2);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(targetMetadata2);

    // Use real ParsedSchema objects - first version matches, second doesn't
    ParsedSchema schema1Version1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2Version1 = new AvroSchema(SCHEMA_STRING_1); // Same as source
    ParsedSchema schema1Version2 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2Version2 = new AvroSchema(SCHEMA_STRING_2); // Different from source
    
    when(sourceClient.parseSchema(any(Schema.class)))
        .thenReturn(Optional.of(schema1Version1))  // Version 1
        .thenReturn(Optional.of(schema1Version2)); // Version 2
    when(targetClient.parseSchema(any(Schema.class)))
        .thenReturn(Optional.of(schema2Version1))  // Version 1 - matches
        .thenReturn(Optional.of(schema2Version2)); // Version 2 - different

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify
    assertFalse(result);
  }

  // Helper methods

  private void setupIdenticalSchemas() throws IOException, RestClientException {
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects with identical schema strings
    ParsedSchema identicalSchema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema identicalSchema2 = new AvroSchema(SCHEMA_STRING_1);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema2));
  }

  private void setupSuccessfulSubjectComparison(String subject) throws IOException, RestClientException {
    when(sourceClient.getAllVersions(subject)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(subject)).thenReturn(Arrays.asList(1));

    SchemaMetadata metadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(subject, 1)).thenReturn(metadata);
    when(targetClient.getSchemaMetadata(subject, 1)).thenReturn(metadata);

    // Use the same schema string so they will be equivalent
    ParsedSchema identicalSchema = new AvroSchema(SCHEMA_STRING_1);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema));
  }

  private void setupFailedSubjectComparison(String subject) throws IOException, RestClientException {
    when(sourceClient.getAllVersions(subject)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(subject)).thenReturn(Arrays.asList(2));
  }

  private SchemaMetadata createSchemaMetadata(int version, int id, String schemaString) {
    return new SchemaMetadata(
        id,
        version,
        AvroSchema.TYPE,
        Collections.emptyList(),
        schemaString
    );
  }

  private boolean invokeCompareSubjects(List<String> sourceSubjects, List<String> targetSubjects) throws Exception {
    Method method = CheckSchemaCompatibility.class.getDeclaredMethod(
        "compareSubjects", List.class, List.class, SchemaRegistryClient.class, SchemaRegistryClient.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(tool, sourceSubjects, targetSubjects, sourceClient, targetClient);
  }

  private boolean invokeCompareSubject(String subject) throws Exception {
    Method method = CheckSchemaCompatibility.class.getDeclaredMethod(
        "compareSubject", SchemaRegistryClient.class, SchemaRegistryClient.class, String.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(tool, sourceClient, targetClient, subject);
  }

  private boolean invokeCompareVersion(String subject, Integer version) throws Exception {
    Method method = CheckSchemaCompatibility.class.getDeclaredMethod(
        "compareVersion", SchemaRegistryClient.class, SchemaRegistryClient.class, String.class, Integer.class);
    method.setAccessible(true);
    return (Boolean) method.invoke(tool, sourceClient, targetClient, subject, version);
  }

  // Tests for compareVersion method

  @Test
  void testCompareVersion_IdenticalVersions() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects with identical schemas - they should be equivalent
    ParsedSchema identicalSchema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema identicalSchema2 = new AvroSchema(SCHEMA_STRING_1);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema2));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertTrue(result);
  }

  @Test
  void testCompareVersion_DifferentSchemas() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_2);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects with different schemas - they should NOT be equivalent
    ParsedSchema schema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2 = new AvroSchema(SCHEMA_STRING_2);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema2));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareVersion_SourceParseFailure() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Mock source parse failure
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.empty());
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(new AvroSchema(SCHEMA_STRING_1)));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareVersion_TargetParseFailure() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 200, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Mock target parse failure
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(new AvroSchema(SCHEMA_STRING_1)));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.empty());

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareVersion_VersionNumberMismatch() throws Exception {
    // Setup - metadata has different version numbers than expected
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(2, 200, SCHEMA_STRING_1); // Different version

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects that are equivalent (same schema content)
    ParsedSchema identicalSchema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema identicalSchema2 = new AvroSchema(SCHEMA_STRING_1);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema2));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareVersion_ExceptionHandling() throws Exception {
    // Setup - throw exception when getting schema metadata
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1))
        .thenThrow(new RestClientException("Connection failed", 500, 50001));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify
    assertFalse(result);
  }

  @Test
  void testCompareVersion_DifferentSchemaIds() throws Exception {
    // Setup - same schemas but different IDs (should still match)
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 999, SCHEMA_STRING_1); // Different ID

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Use real ParsedSchema objects with identical schemas - they should be equivalent
    ParsedSchema identicalSchema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema identicalSchema2 = new AvroSchema(SCHEMA_STRING_1);
    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(identicalSchema2));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - should still return true, just logs the ID difference
    assertTrue(result);
  }
}
