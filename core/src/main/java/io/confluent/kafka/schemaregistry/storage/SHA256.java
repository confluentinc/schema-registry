/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;

public class SHA256 {

  private final byte[] sha256;

  public SHA256(byte[] sha256) {
    if (sha256 == null) {
      throw new IllegalArgumentException(
        "Tried to instantiate SHA256 object with null byte array.");
    }
    if (sha256.length != 32) {
      throw new IllegalArgumentException(
          "Tried to instantiate SHA256 object with invalid byte array."
      );
    }

    this.sha256 = sha256;
  }

  public byte[] bytes() {
    return sha256;
  }

  public static SHA256 ofSchema(Schema schema) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      schema.updateHash(md);
      return new SHA256(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static SHA256 ofSchema(SchemaValue schema) {
    return ofSchema(schema.toSchemaEntity());
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.sha256);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (!(o instanceof SHA256)) {
      return false;
    }

    SHA256 otherSha256 = (SHA256) o;
    return Arrays.equals(this.sha256, otherSha256.sha256);
  }

  public String toHexString() {
    StringBuilder hexString = new StringBuilder();
    for (byte b : sha256) {
      String hex = Integer.toHexString(0xff & b);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
