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

import org.apache.avro.io.BinaryEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GenericBinaryEncoder extends BinaryEncoder {
  private ByteArrayOutputStream out;

  public GenericBinaryEncoder(ByteArrayOutputStream out) {
    this.out = out;
  }

  @Override
  protected void writeZero() throws IOException {

  }

  @Override
  public int bytesBuffered() {
    return 0;
  }

  @Override
  public void writeBoolean(boolean b) throws IOException {

  }

  @Override
  public void writeInt(int i) throws IOException {

  }

  @Override
  public void writeLong(long l) throws IOException {

  }

  @Override
  public void writeFloat(float v) throws IOException {

  }

  @Override
  public void writeDouble(double v) throws IOException {

  }

  @Override
  public void writeFixed(byte[] bytes, int i, int i1) throws IOException {
    out.write(bytes, i, i1);
  }

  @Override
  public void flush() throws IOException {

  }
}
