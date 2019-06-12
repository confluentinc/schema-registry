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

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class SubjectKey extends SchemaRegistryKey {

  private String subject;

  public SubjectKey(@JsonProperty("keytype") SchemaRegistryKeyType keyType,
                    @JsonProperty("subject") String subject) {
    super(keyType);
    this.subject = subject;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return this.subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @Override
  public boolean equals(Object o) {
    if (!super.equals(o)) {
      return false;
    }

    SubjectKey that = (SubjectKey) o;
    if (this.subject != null && that.subject != null) {
      if (!subject.equals(that.subject)) {
        return false;
      }
    } else if (this.subject == null && that.subject == null) {
      return true;
    } else {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    if (this.subject != null) {
      result = 31 * result + subject.hashCode();
    }
    return result;
  }

  @Override
  public int compareTo(SchemaRegistryKey o) {
    int compare = super.compareTo(o);
    if (compare == 0) {
      SubjectKey otherKey = (SubjectKey) o;
      if (this.subject == null && otherKey.getSubject() == null) {
        return 0;
      } else {
        if (this.subject == null) {
          return -1;
        }
        if (otherKey.getSubject() == null) {
          return 1;
        }
        return this.subject.compareTo(otherKey.getSubject());
      }
    } else {
      return compare;
    }
  }
}
