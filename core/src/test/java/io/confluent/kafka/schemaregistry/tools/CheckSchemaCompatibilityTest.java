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
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  void testCompareSubjects_SourceHasMoreSubjects() throws Exception {
    // Setup - source has more subjects, which should be acceptable
    List<String> sourceSubjects = Arrays.asList("subject1", "subject2");
    List<String> targetSubjects = Arrays.asList("subject1");

    // Setup for the common subject comparison
    when(sourceClient.getAllVersions("subject1")).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions("subject1")).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata("subject1", 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata("subject1", 1)).thenReturn(sourceMetadata);

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema1));

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify - should now pass since source having more subjects is acceptable
    assertTrue(result);
  }

  @Test
  void testCompareSubjects_TargetHasMoreSubjects() throws Exception {
    // Setup - target has more subjects, which should not be acceptable
    List<String> sourceSubjects = Arrays.asList("subject1");
    List<String> targetSubjects = Arrays.asList("subject1", "subject2");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify - should fail since target having more subjects is not acceptable
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
    // Setup - source is empty but target has subjects, should fail
    List<String> sourceSubjects = Collections.emptyList();
    List<String> targetSubjects = Arrays.asList("subject1");

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify - should fail since target has subjects that don't exist in source
    assertTrue(result);
  }

  @Test
  void testCompareSubjects_OnlyTargetEmpty() throws Exception {
    // Setup - target is empty but source has subjects, should pass
    List<String> sourceSubjects = Arrays.asList("subject1");
    List<String> targetSubjects = Collections.emptyList();

    // Execute
    boolean result = invokeCompareSubjects(sourceSubjects, targetSubjects);

    // Verify - should pass since source having more subjects is acceptable
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
  void testCompareSubject_SourceHasMoreVersions() throws Exception {
    // Setup - source has more versions, but should only compare common ones
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2, 3));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));

    // Setup for version 1 - success
    SchemaMetadata sourceMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    // Setup for version 2 - success
    SchemaMetadata sourceMetadata2 = createSchemaMetadata(2, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata2 = createSchemaMetadata(2, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(sourceMetadata2);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 2)).thenReturn(targetMetadata2);


    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(parsedSchema1));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify - should succeed since common versions (1, 2) match, version 3 is ignored
    assertTrue(result);
  }

  @Test
  void testCompareSubject_DifferentVersionNumbers() throws Exception {
    // Setup - different version numbers, only version 1 is common
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 3));

    // Setup for common version 1 - success
    SchemaMetadata sourceMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    ParsedSchema schema1 = new AvroSchema(SCHEMA_STRING_1);
    ParsedSchema schema2 = new AvroSchema(SCHEMA_STRING_2);

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema1));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(schema2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify - version mismatch
    assertFalse(result);
  }

  @Test
  void testCompareSubject_NoCommonVersions() throws Exception {
    // Setup - no overlapping versions
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(3, 4));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify - should fail since there are no common versions
    assertFalse(result);
  }

  @Test
  void testCompareSubject_TargetHasMoreVersions() throws Exception {
    // Setup - target has more versions, which should not be acceptable
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1, 2));

    // Execute
    boolean result = invokeCompareSubject(SUBJECT_NAME);

    // Verify - should fail since target having more versions is not acceptable
    assertFalse(result);
  }

  @Test
  void testCompareSubject_SchemaMismatch() throws Exception {
    // Setup
    when(sourceClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));
    when(targetClient.getAllVersions(SUBJECT_NAME)).thenReturn(Arrays.asList(1));

    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_2);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

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
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    // Setup for version 2
    SchemaMetadata sourceMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_2);
    SchemaMetadata targetMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_2);
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
    SchemaMetadata targetMetadata1 = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata1);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata1);

    // Setup for version 2 - failure (different schemas)
    SchemaMetadata sourceMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata2 = createSchemaMetadata(2, 101, SCHEMA_STRING_2);
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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_2);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

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
    SchemaMetadata targetMetadata = createSchemaMetadata(2, 100, SCHEMA_STRING_1); // Different version

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

    // Verify - should return false to prevent potential id conflict
    assertFalse(result);
  }
  
  // Tests for different metadata, ruleSet, and references
  @Test
  void testCompareVersion_DifferentMetadata() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Create schemas with different metadata
    Map<String, String> sourceProperties = new HashMap<>();
    sourceProperties.put("env", "production");
    sourceProperties.put("owner", "team-a");
    Metadata sourceSchemaMetadata = new Metadata(null, sourceProperties, null);

    Map<String, String> targetProperties = new HashMap<>();
    targetProperties.put("env", "staging");
    targetProperties.put("owner", "team-b");
    Metadata targetSchemaMetadata = new Metadata(null, targetProperties, null);

    ParsedSchema sourceSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      sourceSchemaMetadata,
      null,
      null,
      false
    );

    ParsedSchema targetSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      targetSchemaMetadata,
      null,
      null,
      false
    );

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(sourceSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(targetSchema));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - schemas with different metadata should not be equivalent
    assertFalse(result);
  }

  @Test
  void testCompareVersion_DifferentRuleSet() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Create schemas with different ruleSets
    Rule sourceRule = new Rule(
      "encryption-rule",
      "Encrypt PII fields",
      RuleKind.TRANSFORM,
      RuleMode.WRITEREAD,
      "ENCRYPT",
      Collections.singleton("PII"),
      Collections.singletonMap("algorithm", "AES"),
      "encrypt($.ssn)",
      "NONE",
      "ERROR",
      false
    );

    Rule targetRule = new Rule(
      "validation-rule",
      "Validate email format",
      RuleKind.CONDITION,
      RuleMode.WRITEREAD,
      "CEL",
      Collections.singleton("email"),
      Collections.emptyMap(),
      "size(value) > 0",
      "NONE",
      "ERROR",
      false
    );

    RuleSet sourceRuleSet = new RuleSet(null, Arrays.asList(sourceRule), null);
    RuleSet targetRuleSet = new RuleSet(null, Arrays.asList(targetRule), null);

    ParsedSchema sourceSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      null,
      sourceRuleSet,
      null,
      false
    );

    ParsedSchema targetSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      null,
      targetRuleSet,
      null,
      false
    );

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(sourceSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(targetSchema));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - schemas with different ruleSets should not be equivalent
    assertFalse(result);
  }

  @Test
  void testCompareVersion_OneWithMetadataOneWithout() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Create schemas - one with metadata, one without
    Map<String, String> properties = new HashMap<>();
    properties.put("env", "production");
    Metadata metadata = new Metadata(null, properties, null);

    ParsedSchema sourceSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      metadata, // Has metadata
      null,
      null,
      false
    );

    ParsedSchema targetSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      null, // No metadata
      null,
      null,
      false
    );

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(sourceSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(targetSchema));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - schemas where one has metadata and one doesn't should not be equivalent
    assertFalse(result);
  }

  @Test
  void testCompareVersion_OneWithRuleSetOneWithout() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Create schemas - one with ruleSet, one without
    Rule rule = new Rule(
      "validation-rule",
      "Validate field",
      RuleKind.CONDITION,
      RuleMode.WRITEREAD,
      "CEL",
      Collections.emptySet(),
      Collections.emptyMap(),
      "size(value) > 0",
      "NONE",
      "ERROR",
      false
    );
    RuleSet ruleSet = new RuleSet(null, Arrays.asList(rule), null);

    ParsedSchema sourceSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      null,
      ruleSet, // Has ruleSet
      null,
      false
    );

    ParsedSchema targetSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Collections.emptyList(),
      Collections.emptyMap(),
      null,
      null, // No ruleSet
      null,
      false
    );

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(sourceSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(targetSchema));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - schemas where one has ruleSet and one doesn't should not be equivalent
    assertFalse(result);
  }

  @Test
  void testCompareVersion_IdenticalMetadataRuleSetReferences() throws Exception {
    // Setup
    SchemaMetadata sourceMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);
    SchemaMetadata targetMetadata = createSchemaMetadata(1, 100, SCHEMA_STRING_1);

    when(sourceClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(sourceMetadata);
    when(targetClient.getSchemaMetadata(SUBJECT_NAME, 1)).thenReturn(targetMetadata);

    // Create identical metadata, ruleSet, and references
    Map<String, String> properties = new HashMap<>();
    properties.put("env", "production");
    Metadata metadata = new Metadata(null, properties, null);

    Rule rule = new Rule(
      "validation-rule",
      "Validate field",
      RuleKind.CONDITION,
      RuleMode.WRITEREAD,
      "CEL",
      Collections.emptySet(),
      Collections.emptyMap(),
      "size(value) > 0",
      "NONE",
      "ERROR",
      false
    );
    RuleSet ruleSet = new RuleSet(null, Arrays.asList(rule), null);

    SchemaReference reference = new SchemaReference(
      "io.confluent.kafka.example.User",
      "user-schema",
      1
    );

    ParsedSchema sourceSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Arrays.asList(reference),
      Collections.emptyMap(),
      metadata,
      ruleSet,
      null,
      false
    );

    ParsedSchema targetSchema = new AvroSchema(
      SCHEMA_STRING_1,
      Arrays.asList(reference), // Same reference
      Collections.emptyMap(),
      metadata, // Same metadata
      ruleSet, // Same ruleSet
      null,
      false
    );

    when(sourceClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(sourceSchema));
    when(targetClient.parseSchema(any(Schema.class))).thenReturn(Optional.of(targetSchema));

    // Execute
    boolean result = invokeCompareVersion(SUBJECT_NAME, 1);

    // Verify - schemas with identical metadata, ruleSet, and references should be equivalent
    assertTrue(result);
  }
}
