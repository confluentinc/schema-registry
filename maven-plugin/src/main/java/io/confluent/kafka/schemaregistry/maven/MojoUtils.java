/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.maven;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalAvroSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalJsonSchemaProvider;
import io.confluent.kafka.schemaregistry.type.logical.LogicalProtobufSchemaProvider;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

public class MojoUtils {

  public static String readFile(File file, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(file.toPath());
    return new String(encoded, encoding);
  }

  /**
   * The providers used when none are configured. These accept logical types DDL in addition to
   * the native formats, so that a schema authored as DDL can be validated and compared locally.
   * Registering one still sends the DDL itself -- see {@code UploadSchemaRegistryMojo} -- so what
   * gets stored is decided by the registry, not by which providers happen to be configured here.
   */
  public static List<SchemaProvider> defaultSchemaProviders() {
    return Arrays.asList(
        new LogicalAvroSchemaProvider(),
        new LogicalJsonSchemaProvider(),
        new LogicalProtobufSchemaProvider()
    );
  }

}
