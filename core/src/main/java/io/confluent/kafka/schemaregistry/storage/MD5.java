/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Simple wrapper for 16 byte MD5 hash.
 */
public class MD5 {

  private final byte[] md5;

  public MD5(byte[] md5) {
    if (md5 == null) {
      throw new IllegalArgumentException("Tried to instantiate MD5 object with null byte array.");
    }
    if (md5.length != 16) {
      throw new IllegalArgumentException(
          "Tried to instantiate MD5 object with invalid byte array."
      );
    }

    this.md5 = md5;
  }

  public static MD5 fromString(String string) {
    byte[] bytes;
    try {
      UUID uuid = UUID.fromString(string);
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(uuid.getMostSignificantBits());
      bb.putLong(uuid.getLeastSignificantBits());
      bytes = bb.array();
    } catch (IllegalArgumentException e) {
      bytes = HexFormat.of().parseHex(string);
    }
    return new MD5(bytes);
  }

  public byte[] bytes() {
    return md5;
  }

  public static MD5 ofSchemaSimple(Schema schema) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      schema.updateHashSimple(md);
      return new MD5(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static MD5 ofSchemaSimple(SchemaValue schema) {
    byte[] bytes = schema.getMd5Bytes();
    return bytes != null
        ? new MD5(bytes)
        : Schema.updateHashSimple(md, schema.getSchema());
  }

  public static MD5 ofSchema(Schema schema) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      schema.updateHash(md);
      return new MD5(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  public static MD5 ofSchema(SchemaValue schema) {
    byte[] bytes = schema.getMd5Bytes();
    return bytes != null
        ? new MD5(bytes)
        : ofSchema(
            schema.getSchema(),
            schema.getReferences() == null ? null : schema.getReferences().stream()
                .map(SchemaReference::toRefEntity)
                .collect(Collectors.toList()),
            schema.getMetadata() == null ? null : schema.getMetadata().toMetadataEntity() ,
            schema.getRuleSet() == null ? null : schema.getRuleSet().toRuleSetEntity()
        );
  }

  public static MD5 ofSchema(
      String schema,
      List<io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference> references,
      io.confluent.kafka.schemaregistry.client.rest.entities.Metadata metadata,
      io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet ruleSet
  ) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      Schema.updateHash(md, schema, references, metadata, ruleSet);
      return new MD5(md.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(this.md5);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null) {
      return false;
    }

    if (!(o instanceof MD5)) {
      return false;
    }

    MD5 otherMd5 = (MD5) o;
    return Arrays.equals(this.md5, otherMd5.md5);
  }

  @Override
  public String toString() {
    ByteBuffer byteBuffer = ByteBuffer.wrap(md5);
    long high = byteBuffer.getLong();
    long low = byteBuffer.getLong();
    UUID uuid = new UUID(high, low);
    return uuid.toString();
  }
}
