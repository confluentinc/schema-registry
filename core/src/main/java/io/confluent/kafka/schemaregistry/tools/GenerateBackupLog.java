/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.tools;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKeyType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;

public class GenerateBackupLog {
  private static int subjectCounter = 1;
  private static int idCounter = 100001;

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 4) {
      // Currently, the properties file can be found at
      // /config/schema-registry.properties for local runs, and at
      // /opt/confluent/etc/schema-registry/schema-registry.properties in CPD
      System.out.println(
              "Usage: java " + GenerateBackupLog.class.getName() + " properties_file"
                      + " output_file" + " num_schemas" + " num_schema_fields_(optional, default 3)"
      );
      System.exit(1);
    }
    SchemaRegistryConfig config = new SchemaRegistryConfig(args[0]);
    String partition = config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG) + "-0";

    File file = new File(args[1]);
    if (!file.exists() && !file.createNewFile()) {
      System.out.println("failed to create file");
    }
    int numFields = args.length == 4 ? Integer.parseInt(args[3]) : 3;

    FileOutputStream fileStream = new FileOutputStream(file, true);
    OutputStreamWriter fr = new OutputStreamWriter(fileStream, "UTF-8");
    for (int i = 0; i < Integer.parseInt(args[2]); i++) {
      String output = SchemaRegistryKeyType.SCHEMA.toString()
              + "\t"
              + generateSchemaKeyValue(numFields)
              + "\t"
              + partition
              + "\t"
              + System.currentTimeMillis()
              + "\n";
      fr.write(output);
    }
    fr.close();
  }

  private static String generateSchemaKeyValue(int numFields) {
    String name = "test" + subjectCounter++;
    String subject = String.format("%s-value", name);
    StringBuffer sb = new StringBuffer();
    String key = "{\"keytype\":\"SCHEMA\",\"subject\":\""
            + subject
            + "\",\"version\":1,\"magic\":1}\t";
    sb.append(key);

    sb.append("{\"subject\":\"" + subject + "\",\"version\":1,\"id\":" + idCounter++ + ",");
    // schema
    sb.append("\"schema\":\"{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\""
            + name + "\\\", \\\"fields\\\":[");
    for (int i = 0; i < numFields; i++) {
      String schema = String.format("{\\\"name\\\":\\\"field%d\\\",\\\"type\\\":\\\"string\\\"}",
              i);
      sb.append(schema);
      if (i < numFields - 1) {
        sb.append(",");
      }
    }
    sb.append("]}\", \"deleted\":false}");
    return sb.toString();
  }
}
