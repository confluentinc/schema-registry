package io.confluent.kafka.streams.serdes.avro;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class PrimitiveAvroSerdeTest {

  private static final String ANY_TOPIC = "any-topic";

  private static <T> PrimitiveAvroSerde<T>
  createConfiguredSerdeForRecordValues() {
    SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    PrimitiveAvroSerde<T> serde = new PrimitiveAvroSerde<>(schemaRegistryClient);
    Map<String, Object> serdeConfig = new HashMap<>();
    serdeConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "fake");
    serde.configure(serdeConfig, false);
    return serde;
  }

  @Test
  public void shouldRoundTripStringRecords() {
    final PrimitiveAvroSerde<String> stringSerde = createConfiguredSerdeForRecordValues();

    final String stringRecord = "test";

    // When
    final String roundTrippedRecord = stringSerde.deserializer().deserialize(
        ANY_TOPIC,
        stringSerde.serializer().serialize(ANY_TOPIC, stringRecord));

    // Then
    assertThat(roundTrippedRecord, equalTo(stringRecord));

    stringSerde.close();
  }

  @Test
  public void shouldRoundTripLongRecords() {
    final PrimitiveAvroSerde<Long> longSerde = createConfiguredSerdeForRecordValues();

    final Long longRecord = 50000L;

    // When
    Long roundTrippedLongRecord = longSerde.deserializer().deserialize(
        ANY_TOPIC,
        longSerde.serializer().serialize(ANY_TOPIC, longRecord));

    // Then
    assertThat(roundTrippedLongRecord, equalTo(longRecord));

    longSerde.close();
  }

  @Test
  public void shouldRoundTripIntegerRecords() {
    final PrimitiveAvroSerde<Integer> integerSerde = createConfiguredSerdeForRecordValues();

    final Integer intRecord = 50000;

    // When
    Integer roundTrippedIntegerRecord = integerSerde.deserializer().deserialize(
        ANY_TOPIC,
        integerSerde.serializer().serialize(ANY_TOPIC, intRecord));

    // Then
    assertThat(roundTrippedIntegerRecord, equalTo(intRecord));
    integerSerde.close();
  }

  @Test
  public void shouldRoundTripFloatRecords() {
    final PrimitiveAvroSerde<Float> floatSerde = createConfiguredSerdeForRecordValues();

    final Float floatRecord = 50000.0F;

    // When
    Float roundTrippedFloatRecord = floatSerde.deserializer().deserialize(
        ANY_TOPIC,
        floatSerde.serializer().serialize(ANY_TOPIC, floatRecord));

    // Then
    assertThat(roundTrippedFloatRecord, equalTo(floatRecord));
    floatSerde.close();
  }

  @Test
  public void shouldRoundTripDoubleRecords() {
    final PrimitiveAvroSerde<Double> doubleSerde = createConfiguredSerdeForRecordValues();

    final Double doubleRecord = 50000.0D;

    // When
    Double roundTrippedDoubleRecord = doubleSerde.deserializer().deserialize(
        ANY_TOPIC,
        doubleSerde.serializer().serialize(ANY_TOPIC, doubleRecord));

    // Then
    assertThat(roundTrippedDoubleRecord, equalTo(doubleRecord));
    doubleSerde.close();
  }

  @Test
  public void shouldRoundTripBooleanRecords() {
    final PrimitiveAvroSerde<Boolean> booleanSerde = createConfiguredSerdeForRecordValues();

    final Boolean booleanRecord = true;

    // When
    Boolean roundTrippedBooleanRecord = booleanSerde.deserializer().deserialize(
        ANY_TOPIC,
        booleanSerde.serializer().serialize(ANY_TOPIC, booleanRecord));

    // Then
    assertThat(roundTrippedBooleanRecord, equalTo(booleanRecord));
    booleanSerde.close();
  }

  @Test
  public void shouldRoundTripNullRecordsToNull() {
    // Given
    PrimitiveAvroSerde<String> serde = createConfiguredSerdeForRecordValues();

    // When
    String roundtrippedRecord = serde.deserializer().deserialize(
        ANY_TOPIC, serde.serializer().serialize(ANY_TOPIC, null));
    // Then
    assertThat(roundtrippedRecord, nullValue());

    // Cleanup
    serde.close();
  }


  @Test(expected = IllegalArgumentException.class)
  public void shouldFailWhenInstantiatedWithNullSchemaRegistryClient() {
    new PrimitiveAvroSerde<Long>(null);
  }

}