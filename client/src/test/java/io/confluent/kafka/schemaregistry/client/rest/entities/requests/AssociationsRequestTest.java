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

import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.IllegalPropertyException;
import java.util.Collections;
import org.junit.Test;

public class AssociationsRequestTest {

  // AssociationCreateOrUpdateInfo

  @Test(expected = IllegalPropertyException.class)
  public void testInfoNullLifecycleThrows() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, null, null, null, null);
    info.validate(false);
  }

  @Test
  public void testInfoValidLifecycleDoesNotThrow() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    info.validate(false);
  }

  // AssociationCreateOrUpdateOp

  @Test(expected = IllegalPropertyException.class)
  public void testOpNullLifecycleThrows() {
    AssociationCreateOp op = new AssociationCreateOp(
        "test-subject", null, null, null, null, null);
    op.validate(false);
  }

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
    request.validate(false);
  }

  @Test(expected = IllegalPropertyException.class)
  public void testCreateOrUpdateRequestEmptyAssociationsThrows() {
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.emptyList());
    request.validate(false);
  }

  @Test
  public void testCreateOrUpdateRequestValidAssociationsDoesNotThrow() {
    AssociationCreateOrUpdateInfo info = new AssociationCreateOrUpdateInfo(
        "test-subject", null, LifecyclePolicy.STRONG, null, null, null);
    AssociationCreateOrUpdateRequest request = new AssociationCreateOrUpdateRequest(
        "test-resource", "test-ns", "test-id", null, Collections.singletonList(info));
    request.validate(false);
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
}