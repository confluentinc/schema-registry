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
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.io.IOException;

public class ThriftReader implements DatumReader {
  private Schema schema;

  public ThriftReader(Schema schema) {
    this.schema = schema;
  }

  @Override
  public void setSchema(Schema schema) {
  }

  @Override
  public Object read(Object o, Decoder decoder) throws IOException {
    TDeserializer deserializer = new TDeserializer();
    TBase instance;
    try {
      byte[] output = decoder.readBytes(null).array();
      String className = schema.getFullName();
      Class tbaseClass = Class.forName(className);
      instance = (TBase) tbaseClass.newInstance();
      deserializer.deserialize(instance, output);
    } catch (TException | ClassNotFoundException | IllegalAccessException
            | InstantiationException e) {
      throw new IOException("Error reading thrift object", e);
    }
    return instance;
  }
}
