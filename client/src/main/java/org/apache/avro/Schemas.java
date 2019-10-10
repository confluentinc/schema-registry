/**
 * Copyright 2019 Confluent Inc.
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

package org.apache.avro;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collection;

import static org.apache.avro.Schema.FACTORY;

public class Schemas {

  public static String toString(Schema schema, Collection<Schema> schemas) {
    try {
      StringWriter writer = new StringWriter();
      JsonGenerator gen = FACTORY.createGenerator(writer);
      Schema.Names names = new Schema.Names();
      if (schemas != null) {
        for (Schema s : schemas) {
          names.add(s);
        }
      }
      schema.toJson(names, gen);
      gen.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new AvroRuntimeException(e);
    }
  }
}
