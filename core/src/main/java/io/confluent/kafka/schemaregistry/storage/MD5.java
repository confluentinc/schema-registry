/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.kafka.schemaregistry.storage;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import jersey.repackaged.com.google.common.base.Preconditions;

/**
 * Simple wrapper for 16 byte MD5 hash.
 */
public class MD5 {

  private final byte[] md5;

  public MD5(byte[] md5) {
    Preconditions.checkNotNull(md5, "Tried to instantiate MD5 object with null byte array.");
    Preconditions.checkArgument(
        md5.length == 16, "Tried to instantiate MD5 object with invalid byte array.");

    this.md5 = md5;
  }

  /**
   * Factory method converts String into MD5 object.
   */
  public static MD5 ofString(String str) {
    try {
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(str.getBytes());
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
}
