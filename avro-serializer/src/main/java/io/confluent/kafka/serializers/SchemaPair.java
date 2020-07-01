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

package io.confluent.kafka.serializers;

import org.apache.avro.Schema;

import java.util.Objects;

public class SchemaPair {
  private final Schema writerSchema;
  private final Schema readerSchema;

  public SchemaPair(Schema writerSchema, Schema readerSchema) {
    this.writerSchema = writerSchema;
    this.readerSchema = readerSchema;
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  public Schema getReaderSchema() {
    return readerSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaPair that = (SchemaPair) o;
    return Objects.equals(writerSchema, that.writerSchema)
        && Objects.equals(readerSchema, that.readerSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(writerSchema, readerSchema);
  }
}
