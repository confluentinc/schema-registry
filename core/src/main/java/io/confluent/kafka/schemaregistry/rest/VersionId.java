/*
 * Copyright 2015-2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;

/**
 * A valid version id should be a positive integer between 1 and 2^31-1. -1 is a special version id
 * that indicates the "latest" version under a subject
 */
public class VersionId {

  private final int version;

  public VersionId(String version) throws InvalidVersionException {
    if (version.trim().toLowerCase().equals("latest")) {
      this.version = -1;
    } else {
      try {
        this.version = Integer.parseInt(version.trim());
      } catch (NumberFormatException nfe) {
        throw new InvalidVersionException(version);
      }
      assertValidVersion();
    }
  }

  public VersionId(int version) throws InvalidVersionException {
    this.version = version;
    assertValidVersion();
  }

  public int getVersionId() {
    return this.version;
  }

  public boolean isLatest() {
    return version == -1;
  }

  private void assertValidVersion() throws InvalidVersionException {
    if (this.version <= 0 && this.version != -1) {
      throw new InvalidVersionException(String.valueOf(this.version));
    }
  }
}
