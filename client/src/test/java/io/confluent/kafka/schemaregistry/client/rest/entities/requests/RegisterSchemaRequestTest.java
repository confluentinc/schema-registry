/*
 * Copyright 2019-2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class RegisterSchemaRequestTest {

  @Test
  public void buildRegisterSchemaRequest() throws Exception {

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("string");
    request.setId(100);
    request.setVersion(10);

    assertEquals("{\"version\":10,\"id\":100,\"schema\":\"string\"}", request.toJson());
  }

  @Test
  public void buildRegisterSchemaRequestWithoutId() throws Exception {

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("string");
    request.setVersion(10);

    assertEquals("{\"version\":10,\"schema\":\"string\"}", request.toJson());
  }

  @Test
  public void buildRegisterSchemaRequestWithoutVersion() throws Exception {

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("string");
    request.setId(100);

    assertEquals("{\"id\":100,\"schema\":\"string\"}", request.toJson());
  }

  @Test
  public void buildRegisterSchemaRequestWithoutIdOrVersion() throws Exception {

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("string");

    assertEquals("{\"schema\":\"string\"}", request.toJson());
  }

  @Test
  public void buildRegisterSchemaRequestWithSchemaTypeAvro() throws Exception {

    RegisterSchemaRequest request = new RegisterSchemaRequest();
    request.setSchema("string");
    request.setSchemaType("AVRO");

    // Note that schemaType is omitted
    assertEquals("{\"schema\":\"string\"}", request.toJson());
  }
}
