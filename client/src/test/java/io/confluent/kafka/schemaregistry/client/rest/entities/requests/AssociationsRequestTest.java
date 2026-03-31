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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import java.util.Collections;
import org.junit.Test;

public class AssociationsRequestTest {

  // AssociationCreateOrUpdateInfo

  @Test
  public void testInfoValidLifecycleDoesNotThrow() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    info.validate(false, false);
  }

  // AssociationCreateOrUpdateOp

  @Test
  public void testOpValidLifecycleDoesNotThrow() {
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    op.validate(false);
  }

  // AssociationCreateOrUpdateRequest

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOrUpdateRequestNullAssociationsThrows() {
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, null);
    request.validate(false, false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOrUpdateRequestEmptyAssociationsThrows() {
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.emptyList());
    request.validate(false, false);
  }

  @Test
  public void testCreateOrUpdateRequestValidAssociationsDoesNotThrow() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(false, false);
  }

  // AssociationOpRequest

  @Test(expected = IllegalPropertyException.class)
  public void testOpRequestNullAssociationsThrows() {
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, null);
    request.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testOpRequestEmptyAssociationsThrows() {
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.emptyList());
    request.validate(false);
  }

  @Test
  public void testOpRequestValidAssociationsDoesNotThrow() {
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
  }

  // Requirement #1: CREATE + schema → STRONG + frozen

  @Test
  public void testCreateOpWithSchemaDefaultsToStrongFrozen() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, null, null, schema, null);
    op.validate(false);
    assertEquals(LifecyclePolicy.STRONG, op.getLifecycle());
    assertTrue(op.getFrozen());
  }

  @Test
  public void testCreateOpWithSchemaAndExplicitStrongFrozenDoesNotThrow() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, LifecyclePolicy.STRONG, true, schema, null);
    op.validate(false);
    assertEquals(LifecyclePolicy.STRONG, op.getLifecycle());
    assertTrue(op.getFrozen());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpWithSchemaAndWeakLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, LifecyclePolicy.WEAK, null, schema, null);
    op.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpWithSchemaAndFrozenFalseThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, null, false, schema, null);
    op.validate(false);
  }

  @Test
  public void testCreateInfoWithSchemaDefaultsToStrongFrozen() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, null, null, schema, null);
    info.validate(true, false);
    assertEquals(LifecyclePolicy.STRONG, info.getLifecycle());
    assertTrue(info.getFrozen());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoWithSchemaAndWeakLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.WEAK, null, schema, null);
    info.validate(true, false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoWithSchemaAndFrozenFalseThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, null, false, schema, null);
    info.validate(true, false);
  }

  // Requirement #2: UPSERT + schema → only STRONG

  @Test(expected = IllegalPropertyException.class)
  public void testUpsertOpWithSchemaAndWeakLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationUpsertOp op = new AssociationUpsertOp(
        "test-subject", null, LifecyclePolicy.WEAK, null, schema, null);
    op.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testUpsertOpWithSchemaAndDefaultLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationUpsertOp op = new AssociationUpsertOp(
        "test-subject", null, null, null, schema, null);
    op.validate(false);
  }

  @Test
  public void testUpsertOpWithSchemaAndStrongLifecycleDoesNotThrow() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationUpsertOp op = new AssociationUpsertOp(
        "test-subject", null, LifecyclePolicy.STRONG, null, schema, null);
    op.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testUpsertInfoWithSchemaAndWeakLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.WEAK, null, schema, null);
    info.validate(false, false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testUpsertInfoWithSchemaAndDefaultLifecycleThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, null, null, schema, null);
    info.validate(false, false);
  }

  // Requirement #3: Default subject + subject required

  @Test
  public void testCreateOpWithNullSubjectAndFrozenStrongDoesNotThrow() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        null, null, null, null, schema, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
    assertEquals(":.test-ns:test-resource", op.getSubject());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpWithNullSubjectAndWeakThrows() {
    AssociationCreateOp op = new AssociationCreateOp(
        null, null, LifecyclePolicy.WEAK, null, null, null);
    op.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpWithNullSubjectAndNonFrozenStrongThrows() {
    AssociationCreateOp op = new AssociationCreateOp(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    op.validate(false);
  }

  @Test
  public void testCreateInfoWithNullSubjectAndFrozenStrongDoesNotThrow() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, null, null, schema, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(true, false);
    assertEquals(":.test-ns:test-resource", info.getSubject());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoWithNullSubjectAndWeakThrows() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.WEAK, null, null, null);
    info.validate(true, false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoWithNullSubjectAndNonFrozenStrongThrows() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    info.validate(true, false);
  }

  @Test
  public void testUpsertOpWithNullSubjectDefaultsSubject() {
    AssociationUpsertOp op = new AssociationUpsertOp(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
    assertEquals(":.test-ns:test-resource", op.getSubject());
  }

  @Test
  public void testUpsertInfoWithNullSubjectDefaultsSubject() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(false, false);
    assertEquals(":.test-ns:test-resource", info.getSubject());
  }

  @Test
  public void testBatchRequestValidateCallsOpRequestValidate() {
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    AssociationOpRequest opRequest = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    AssociationBatchRequest batchRequest = new AssociationBatchRequest(
        Collections.singletonList(opRequest));
    batchRequest.validate(false);
  }

  @Test
  public void testCheckSubjectValidatesUserProvidedSubject() {
    AssociationCreateOp op = new AssociationCreateOp(
        "valid-subject", null, LifecyclePolicy.STRONG, null, null, null);
    op.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCheckSubjectRejectsControlCharsInSubject() {
    AssociationCreateOp op = new AssociationCreateOp(
        "bad\u0000subject", null, LifecyclePolicy.STRONG, null, null, null);
    op.validate(false);
  }
}
