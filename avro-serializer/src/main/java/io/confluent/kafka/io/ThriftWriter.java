/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.io;

import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.apache.avro.thrift.ThriftData;
import org.apache.avro.thrift.ThriftDatumWriter;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.io.IOException;

public class ThriftWriter<T> extends ThriftDatumWriter<T> {

  public ThriftWriter(Class<T> c) {
    super(ThriftData.get().getSchema(c), ThriftData.get());
  }

  @Override
  protected void write(Schema schema, Object datum, Encoder out) throws IOException {
    byte[] output;
    TSerializer serializer = new TSerializer();
    try {
      output = serializer.serialize((TBase) datum);
    } catch (TException e) {
      throw new IOException("Error writing thrift object", e);
    }
    out.writeBytes(output, 0, output.length);
  }
}
