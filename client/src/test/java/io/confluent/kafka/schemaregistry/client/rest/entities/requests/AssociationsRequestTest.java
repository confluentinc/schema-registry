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

  @Test
  public void testUpsertOpWithSchemaAndDefaultLifecycleDoesNotThrow() {
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

  @Test
  public void testUpsertInfoWithSchemaAndDefaultLifecycleDoesNotThrow() {
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
    assertEquals(":.test-ns:test-resource-value", op.getSubject());
  }

  @Test
  public void testCreateOpWithNullSubjectAndNonFrozenStrongDoesNotThrow() {
    AssociationCreateOp op = new AssociationCreateOp(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
    assertEquals(":.test-ns:test-resource-value", op.getSubject());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpWithNullSubjectAndWeakThrows() {
    AssociationCreateOp op = new AssociationCreateOp(
        null, null, LifecyclePolicy.WEAK, null, null, null);
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
    assertEquals(":.test-ns:test-resource-value", info.getSubject());
  }

  @Test
  public void testCreateInfoWithNullSubjectAndNonFrozenStrongDoesNotThrow() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(true, false);
    assertEquals(":.test-ns:test-resource-value", info.getSubject());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoWithNullSubjectAndWeakThrows() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.WEAK, null, null, null);
    info.validate(true, false);
  }

  @Test
  public void testUpsertOpWithNullSubjectDefaultsSubject() {
    AssociationUpsertOp op = new AssociationUpsertOp(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
    assertEquals(":.test-ns:test-resource-value", op.getSubject());
  }

  @Test
  public void testUpsertInfoWithNullSubjectDefaultsSubject() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        null, null, LifecyclePolicy.STRONG, null, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(false, false);
    assertEquals(":.test-ns:test-resource-value", info.getSubject());
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

  // Requirement: Frozen STRONG must use default subject

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpFrozenWithCustomSubjectThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        "custom-subject", null, null, null, schema, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
  }

  @Test
  public void testCreateOpFrozenWithExplicitDefaultSubjectSucceeds() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp op = new AssociationCreateOp(
        ":.test-ns:test-resource-value", null, null, null, schema, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
    assertEquals(":.test-ns:test-resource-value", op.getSubject());
  }

  @Test(expected = IllegalPropertyException.class)
  public void testUpsertOpFrozenWithCustomSubjectThrows() {
    AssociationUpsertOp op = new AssociationUpsertOp(
        "custom-subject", null, LifecyclePolicy.STRONG, true, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(op));
    request.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoFrozenWithCustomSubjectThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "custom-subject", null, null, null, schema, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(true, false);
  }

  @Test
  public void testCreateInfoFrozenWithExplicitDefaultSubjectSucceeds() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        ":.test-ns:test-resource-value", null, null, null, schema, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(true, false);
    assertEquals(":.test-ns:test-resource-value", info.getSubject());
  }

  // Requirement: Frozen/non-frozen consistency at resource level

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOpMixedFrozenAndNonFrozenThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOp frozenOp = new AssociationCreateOp(
        null, "key", null, null, schema, null);
    AssociationCreateOp nonFrozenOp = new AssociationCreateOp(
        "test-subject", "value", LifecyclePolicy.STRONG, false, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null,
        java.util.Arrays.asList(frozenOp, nonFrozenOp));
    request.validate(false);
  }

  @Test
  public void testCreateOpAllFrozenSucceeds() {
    RegisterSchemaRequest schema1 = new RegisterSchemaRequest();
    schema1.setSchema("{\"type\":\"string\"}");
    RegisterSchemaRequest schema2 = new RegisterSchemaRequest();
    schema2.setSchema("{\"type\":\"int\"}");
    AssociationCreateOp op1 = new AssociationCreateOp(
        null, "key", null, null, schema1, null);
    AssociationCreateOp op2 = new AssociationCreateOp(
        null, "value", null, null, schema2, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null,
        java.util.Arrays.asList(op1, op2));
    request.validate(false);
  }

  @Test
  public void testCreateOpAllNonFrozenSucceeds() {
    AssociationCreateOp op1 = new AssociationCreateOp(
        "subject1", "key", LifecyclePolicy.STRONG, false, null, null);
    AssociationCreateOp op2 = new AssociationCreateOp(
        "subject2", "value", LifecyclePolicy.STRONG, false, null, null);
    AssociationOpRequest request = new AssociationOpRequest(
        "test-resource", "test-ns", "test-id", null,
        java.util.Arrays.asList(op1, op2));
    request.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateInfoMixedFrozenAndNonFrozenThrows() {
    RegisterSchemaRequest schema = new RegisterSchemaRequest();
    schema.setSchema("{\"type\":\"string\"}");
    AssociationCreateOrUpdateInfo frozenInfo = new AssociationCreateOrUpdateInfo(
        null, "key", null, null, schema, null);
    AssociationCreateOrUpdateInfo nonFrozenInfo = new AssociationCreateOrUpdateInfo(
        "test-subject", "value", LifecyclePolicy.STRONG, false, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null,
        java.util.Arrays.asList(frozenInfo, nonFrozenInfo));
    request.validate(true, false);
  }

  @Test
  public void testCreateInfoAllFrozenSucceeds() {
    RegisterSchemaRequest schema1 = new RegisterSchemaRequest();
    schema1.setSchema("{\"type\":\"string\"}");
    RegisterSchemaRequest schema2 = new RegisterSchemaRequest();
    schema2.setSchema("{\"type\":\"int\"}");
    AssociationCreateOrUpdateInfo info1 = new AssociationCreateOrUpdateInfo(
        null, "key", null, null, schema1, null);
    AssociationCreateOrUpdateInfo info2 = new AssociationCreateOrUpdateInfo(
        null, "value", null, null, schema2, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null,
        java.util.Arrays.asList(info1, info2));
    request.validate(true, false);
  }
}
