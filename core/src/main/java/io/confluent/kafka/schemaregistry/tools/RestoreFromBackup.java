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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.ClearSubjectKey;
import io.confluent.kafka.schemaregistry.storage.ClearSubjectValue;
import io.confluent.kafka.schemaregistry.storage.ConfigKey;
import io.confluent.kafka.schemaregistry.storage.ConfigValue;
import io.confluent.kafka.schemaregistry.storage.DeleteSubjectKey;
import io.confluent.kafka.schemaregistry.storage.DeleteSubjectValue;
import io.confluent.kafka.schemaregistry.storage.ModeKey;
import io.confluent.kafka.schemaregistry.storage.ModeValue;
import io.confluent.kafka.schemaregistry.storage.SchemaKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKey;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryKeyType;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryValue;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.kafka.schemaregistry.storage.serialization.Serializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class RestoreFromBackup {
  private static final Serializer<SchemaRegistryKey, SchemaRegistryValue> serializer =
          new SchemaRegistrySerializer();

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      // Currently, the properties file can be found at
      // /config/schema-registry.properties for local runs, and at
      // /opt/confluent/etc/schema-registry/schema-registry.properties in CPD
      System.out.println(
              "Usage: java " + RestoreFromBackup.class.getName() + " properties_file"
                      + " backup_file"
      );
      System.exit(1);
    }

    SchemaRegistryConfig config = new SchemaRegistryConfig(args[0]);
    Properties props = new Properties();
    props.putAll(config.originalsWithPrefix("kafkastore."));
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapBrokers());
    props.put(ProducerConfig.ACKS_CONFIG, "-1");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArraySerializer.class);
    props.put(ProducerConfig.RETRIES_CONFIG, 0); // Producer should not retry
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "backup-restore-producer");
    KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

    List<ProducerRecord<byte[], byte[]>> records = getSchemaRecords(args[1], config);
    for (ProducerRecord<byte[], byte[]> record : records) {
      producer.send(record).get();
    }
    System.out.println("Restore complete.");
  }

  // Public for testing
  public static List<ProducerRecord<byte[], byte[]>> getSchemaRecords(
          String filePath, SchemaRegistryConfig config) throws Exception {
    List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    Scanner scanner = new Scanner(new File(filePath), "UTF-8");
    while (scanner.hasNextLine()) {
      String line = scanner.nextLine();
      String[] tokens = line.split("\t");
      if (tokens.length != 5) {
        System.out.println(
                String.format("wrong number of parts for line: expected 5, got %d: \"%s\"",
                tokens.length, line));
        break;
      }
      SchemaRegistryKeyType type = SchemaRegistryKeyType.forName(tokens[0]);
      if (type == SchemaRegistryKeyType.NOOP) {
        // nothing to handle for no-ops
        continue;
      }
      ObjectMapper obj = new ObjectMapper();
      SchemaRegistryKey key = keyFromType(obj, tokens[1], type);
      SchemaRegistryValue value = valueFromType(obj, tokens[2], type);
      String tp = tokens[3];
      int partitionIndex = tp.lastIndexOf("-");
      if (partitionIndex == -1) {
        throw new IllegalArgumentException("topic partition was not well formed: " + tp);
      }
      long timestamp = Long.parseLong(tokens[4]);
      records.add(new ProducerRecord<>(
              config.getString(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG),
              Integer.parseInt(tp.substring(partitionIndex + 1)),
              timestamp,
              serializer.serializeKey(key),
              value == null ? null : serializer.serializeValue(value)));
    }
    scanner.close();
    return records;
  }

  private static SchemaRegistryKey keyFromType(ObjectMapper obj,
                                               String key,
                                               SchemaRegistryKeyType type)
          throws JsonProcessingException, IllegalArgumentException {
    switch (type) {
      case CLEAR_SUBJECT:
        return obj.readValue(key, ClearSubjectKey.class);
      case CONFIG:
        return obj.readValue(key, ConfigKey.class);
      case DELETE_SUBJECT:
        return obj.readValue(key, DeleteSubjectKey.class);
      case MODE:
        return obj.readValue(key, ModeKey.class);
      case SCHEMA:
        return obj.readValue(key, SchemaKey.class);
      default:
        throw new IllegalArgumentException("Unknown schema registry key type : " + type);
    }
  }

  private static SchemaRegistryValue valueFromType(ObjectMapper obj,
                                                   String key,
                                                   SchemaRegistryKeyType type)
          throws JsonProcessingException, IllegalArgumentException {
    switch (type) {
      case CLEAR_SUBJECT:
        return obj.readValue(key, ClearSubjectValue.class);
      case CONFIG:
        return obj.readValue(key, ConfigValue.class);
      case DELETE_SUBJECT:
        return obj.readValue(key, DeleteSubjectValue.class);
      case MODE:
        return obj.readValue(key, ModeValue.class);
      case SCHEMA:
        return obj.readValue(key, SchemaValue.class);
      default:
        throw new IllegalArgumentException("Unknown schema registry key type : " + type);
    }
  }
}
