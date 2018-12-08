/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class ModeKeyAndValue {

  private String subject;
  private boolean prefix;
  private Mode mode;

  public ModeKeyAndValue(@JsonProperty("subject") String subject,
                         @JsonProperty("prefix") boolean prefix,
                         @JsonProperty("mode") Mode mode) {
    this.subject = subject;
    this.prefix = prefix;
    this.mode = mode;
  }

  public ModeKeyAndValue(ModeKey modeKey, ModeValue modeValue) {
    this.subject = modeKey.getSubject();
    this.prefix = modeKey.isPrefix();
    this.mode = modeValue.getMode();
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonProperty("prefix")
  public boolean isPrefix() {
    return this.prefix;
  }

  @JsonProperty("prefix")
  public void setPrefix(boolean prefix) {
    this.prefix = prefix;
  }

  @JsonProperty("mode")
  public Mode getMode() {
    return this.mode;
  }

  @JsonProperty("mode")
  public void setMode(Mode mode) {
    this.mode = mode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ModeKeyAndValue that = (ModeKeyAndValue) o;
    return prefix == that.prefix && Objects.equals(subject, that.subject) && Objects.equals(
        mode,
        that.mode
    );
  }

  @Override
  public int hashCode() {
    return Objects.hash(subject, prefix, mode);
  }

  @Override
  public String toString() {
    return "ModeKeyAndValue{"
        + "subject='"
        + subject
        + '\''
        + ", prefix="
        + prefix
        + ", mode='"
        + mode
        + '\''
        + '}';
  }
}
