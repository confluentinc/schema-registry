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
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtobufReader implements DatumReader {
  private Schema schema;

  public ProtobufReader(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void setSchema(Schema schema) {
  }

  @Override
  public Object read(Object o, Decoder decoder) throws IOException {
    Object instance;
    try {
      ByteArrayInputStream output = new ByteArrayInputStream(decoder.readBytes(null).array());
      String className = schema.getNamespace() + schema.getName();
      Class generatedMessageClass = Class.forName(className);
      Method newBuilderMethod = generatedMessageClass.getMethod("newBuilder");
      Object builder = newBuilderMethod.invoke(null);
      Method mergeFromMethod = builder.getClass().getMethod("mergeFrom", InputStream.class);
      mergeFromMethod.invoke(builder, output);
      Method buildMethod = builder.getClass().getMethod("build");
      instance = buildMethod.invoke(builder);
    } catch (NoSuchMethodException | ClassNotFoundException
        | IllegalAccessException | InvocationTargetException e) {
      throw new IOException("Error reading protobuf object", e);
    }
    return instance;
  }
}
