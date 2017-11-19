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
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;

public class ProtobufWriter<T> extends ProtobufDatumWriter<T> {

  public ProtobufWriter(Class<T> c) {
      super(ProtobufData.get().getSchema(c), ProtobufData.get());
  }

  @Override
  protected void write(Schema schema, Object datum, Encoder out) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    java.lang.reflect.Method method;
    try {
      method = datum.getClass().getMethod("writeTo", OutputStream.class);
      method.invoke(datum, output);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IOException("Error writing protobuf object", e);
    }
    out.writeBytes(output.toByteArray(), 0, output.toByteArray().length);
  }
}
