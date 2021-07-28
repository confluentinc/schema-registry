/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.schemaregistry.benchmark;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.schemaregistry.tools.SchemaRegistryPerformance;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 *  Runs JMH microbenchmarks against serdes.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 6, time = 30)
@Measurement(iterations = 3, time = 60)
@Threads(4)
@Fork(3)
public class SerdeBenchmark {

  private static final String TOPIC_NAME = "serde_benchmark";

  @State(Scope.Thread)
  public static class SerdeState {

    Serializer serializer;
    Deserializer deserializer;
    ParsedSchema schema;
    Object row;
    byte[] bytes;

    @Param({"AVRO", "JSON", "PROTOBUF"})
    public String serializationFormat;

    @Setup(Level.Iteration)
    public void setUp() throws IOException {
      final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
      final ImmutableMap<String, Object> configs = ImmutableMap.of(
          AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true,
          AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ""
      );

      switch (serializationFormat) {
        case "AVRO":
          serializer = new KafkaAvroSerializer(schemaRegistryClient, configs);
          deserializer = new KafkaAvroDeserializer(schemaRegistryClient, configs);
          break;
        case "JSON":
          serializer = new KafkaJsonSchemaSerializer(schemaRegistryClient, configs);
          deserializer = new KafkaJsonSchemaDeserializer(
              schemaRegistryClient, configs, Record.class);
          break;
        case "PROTOBUF":
          serializer = new KafkaProtobufSerializer(schemaRegistryClient, configs);
          deserializer = new KafkaProtobufDeserializer(schemaRegistryClient, configs);
          break;
        default:
          throw new RuntimeException("Invalid format: " + serializationFormat);
      }
      schema  = SchemaRegistryPerformance.makeParsedSchema(serializationFormat, 1);
      row  = makeRecord(schema);
      bytes = serializer.serialize(TOPIC_NAME, row);
    }
  }

  public static Object makeRecord(ParsedSchema schema) throws IOException {
    String jsonString = "{\"f1\": \"foo\"}";
    switch (schema.schemaType()) {
      case AvroSchema.TYPE:
        return AvroSchemaUtils.toObject(jsonString, (AvroSchema) schema);
      case JsonSchema.TYPE:
        return new Record("foo");
      case ProtobufSchema.TYPE:
        return ProtobufSchemaUtils.toObject(jsonString, (ProtobufSchema) schema);
      default:
        throw new IllegalArgumentException("Unsupported schema type " + schema.schemaType());
    }
  }

  static class Record {
    Record() {

    }

    Record(String f1) {
      this.f1 = f1;
    }

    @JsonProperty
    String f1;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Record record = (Record) o;
      return Objects.equals(f1, record.f1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(f1);
    }
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public byte[] serialize(final SerdeState serdeState) {
    return serdeState.serializer.serialize(TOPIC_NAME, serdeState.row);
  }

  @SuppressWarnings("MethodMayBeStatic") // Tests can not be static
  @Benchmark
  public Object deserialize(final SerdeState serdeState) {
    return serdeState.deserializer.deserialize(TOPIC_NAME, serdeState.bytes);
  }

  public static void main(final String[] args) throws Exception {

    final Options opt = args.length != 0
        ? new CommandLineOptions(args)
        : new OptionsBuilder()
            .include(SerdeBenchmark.class.getSimpleName())
            .shouldFailOnError(true)
            .build();

    new Runner(opt).run();
  }
}
