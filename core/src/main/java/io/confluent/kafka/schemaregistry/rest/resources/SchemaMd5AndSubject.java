/*
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
 */

package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.storage.MD5;

public class SchemaMd5AndSubject {

  private MD5 md5;
  private String subject;
  
  public SchemaMd5AndSubject(String subject, MD5 md5) {
    this.subject = subject;  
    this.md5 = md5;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaMd5AndSubject that = (SchemaMd5AndSubject) o;

    if (!this.subject.equals(that.subject)) {
      return false;
    }
    if (!this.md5.equals(that.md5)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = subject.hashCode();
    result = 31 * result + md5.hashCode();
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{subject=" + this.subject + ",");
    sb.append("md5=" + this.md5.toString() + "}");
    return sb.toString();
  }

}
