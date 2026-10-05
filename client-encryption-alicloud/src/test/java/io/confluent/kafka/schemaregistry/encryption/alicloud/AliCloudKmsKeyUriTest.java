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

package io.confluent.kafka.schemaregistry.encryption.alicloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.security.GeneralSecurityException;
import org.junit.Test;

public class AliCloudKmsKeyUriTest {

  @Test
  public void parsesRegionAndDecodedKeyId() throws Exception {
    AliCloudKmsKeyUri uri = AliCloudKmsKeyUri.parse("alicloud-kms://cn-chengdu/alias%2Fcsfle");

    assertEquals("alicloud-kms://cn-chengdu/alias%2Fcsfle", uri.original());
    assertEquals("cn-chengdu", uri.regionId());
    assertEquals("alias/csfle", uri.keyId());
  }

  @Test
  public void rejectsUnsupportedScheme() {
    assertThrows(
        GeneralSecurityException.class,
        () -> AliCloudKmsKeyUri.parse("aws-kms://cn-chengdu/alias%2Fcsfle"));
    assertThrows(
        GeneralSecurityException.class,
        () -> AliCloudKmsKeyUri.parse("aliyun-kms://cn-chengdu/alias%2Fcsfle"));
  }

  @Test
  public void rejectsMissingKeyId() {
    assertThrows(
        GeneralSecurityException.class,
        () -> AliCloudKmsKeyUri.parse("alicloud-kms://cn-chengdu"));
  }
}
