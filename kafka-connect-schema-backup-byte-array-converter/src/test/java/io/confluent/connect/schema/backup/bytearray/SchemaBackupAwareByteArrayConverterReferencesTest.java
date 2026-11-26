/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.schema.backup.bytearray;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import io.confluent.connect.schema.backup.api.SchemaBackupConfig;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Before;
import org.junit.Test;

/**
 * Schema references for all three formats, transitive and shared references, and the
 * key side of {@link SchemaBackupAwareByteArrayConverter}.
 */
public class SchemaBackupAwareByteArrayConverterReferencesTest {

  private static final String SOURCE_TOPIC = "payments";
  private static final String RESTORE_TOPIC = "restored-payments";
  private static final String SECOND_SOURCE_TOPIC = "invoices";
  private static final String SECOND_RESTORE_TOPIC = "restored-invoices";
  private static final String VALUE_SUFFIX = "-value";
  private static final String KEY_SUFFIX = "-key";
  private static final String SR_URL_CONFIG = AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
  private static final String SR_URL_VALUE = "http://fake-url";
  private static final String TRUE = "true";
  private static final int PREFIX_LENGTH = 5;
  private static final int FIRST_VERSION = 1;
  /** Protobuf message-index encoding for the first message in the file. */
  private static final byte FIRST_MESSAGE_INDEX = 0;

  private static final String NAMESPACE = "io.confluent.test";
  private static final String FIELD_ID = "id";
  private static final String FIELD_CITY = "city";
  private static final String FIELD_CODE = "code";
  private static final String FIELD_ADDRESS = "address";
  private static final String FIELD_SHIP_TO = "shipTo";
  private static final String FIELD_AMOUNT = "amount";
  private static final String FIELD_CURRENCY = "currency";
  private static final String FIELD_UNITS = "units";
  private static final String ID_VALUE = "id-1";
  private static final String CITY_VALUE = "Chennai";
  private static final String CODE_VALUE = "IN";
  private static final String CURRENCY_VALUE = "INR";
  private static final long UNITS_VALUE = 250L;

  // ---- Avro: Order -> Address -> Country (transitive), Invoice -> Address (shared) ----
  private static final String COUNTRY_NAME = NAMESPACE + ".Country";
  private static final String ADDRESS_NAME = NAMESPACE + ".Address";
  private static final String COUNTRY_SUBJECT = "country";
  private static final String ADDRESS_SUBJECT = "address";
  private static final String COUNTRY_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Country\",\"namespace\":\"" + NAMESPACE + "\","
          + "\"fields\":[{\"name\":\"" + FIELD_CODE + "\",\"type\":\"string\"}]}";
  private static final String ADDRESS_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"" + NAMESPACE + "\","
          + "\"fields\":["
          + "{\"name\":\"" + FIELD_CITY + "\",\"type\":\"string\"},"
          + "{\"name\":\"" + FIELD_CODE + "\",\"type\":\"" + COUNTRY_NAME + "\"}"
          + "]}";
  private static final String ORDER_SCHEMA = recordWithAddress("Order");
  private static final String INVOICE_SCHEMA = recordWithAddress("Invoice");

  // ---- Protobuf: Payment imports Money ----
  private static final String MONEY_PROTO_NAME = "money.proto";
  private static final String MONEY_SUBJECT = "money-proto";
  private static final String MONEY_PROTO =
      "syntax = \"proto3\";\n"
          + "package " + NAMESPACE + ";\n"
          + "message Money {\n"
          + "  string " + FIELD_CURRENCY + " = 1;\n"
          + "  int64 " + FIELD_UNITS + " = 2;\n"
          + "}\n";
  private static final String PAYMENT_PROTO =
      "syntax = \"proto3\";\n"
          + "package " + NAMESPACE + ";\n"
          + "import \"" + MONEY_PROTO_NAME + "\";\n"
          + "message Payment {\n"
          + "  string " + FIELD_ID + " = 1;\n"
          + "  Money " + FIELD_AMOUNT + " = 2;\n"
          + "}\n";

  // ---- JSON Schema: Shipment $ref address.json ----
  private static final String ADDRESS_JSON_NAME = "address.json";
  private static final String ADDRESS_JSON_SUBJECT = "address-json";
  private static final String ADDRESS_JSON_SCHEMA =
      "{\"$id\":\"" + ADDRESS_JSON_NAME + "\",\"type\":\"object\","
          + "\"properties\":{\"" + FIELD_CITY + "\":{\"type\":\"string\"}}}";
  private static final String SHIPMENT_JSON_SCHEMA =
      "{\"type\":\"object\",\"properties\":{"
          + "\"" + FIELD_ID + "\":{\"type\":\"string\"},"
          + "\"" + FIELD_SHIP_TO + "\":{\"$ref\":\"" + ADDRESS_JSON_NAME + "\"}}}";
  private static final String SHIPMENT_JSON =
      "{\"" + FIELD_ID + "\":\"" + ID_VALUE + "\",\"" + FIELD_SHIP_TO + "\":{\""
          + FIELD_CITY + "\":\"" + CITY_VALUE + "\"}}";

  private RecordingClient source;
  private RecordingClient target;

  @Before
  public void setUp() {
    source = new RecordingClient();
    target = new RecordingClient();
  }

  // ---------------- References per format ----------------

  @Test
  public void testAvroTransitiveReferencesRegisteredDeepestFirst() throws Exception {
    byte[] original = avroRecordBytes(SOURCE_TOPIC, ORDER_SCHEMA);
    SchemaBackupAwareByteArrayConverter restore = newConverter(target, false);

    byte[] restored = restore(newConverter(source, false), restore,
        SOURCE_TOPIC, RESTORE_TOPIC, original);

    assertEquals(Arrays.asList(COUNTRY_SUBJECT, ADDRESS_SUBJECT, RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
    assertPayloadUnchanged(original, restored);
    GenericRecord record = (GenericRecord) avroDeserializer().deserialize(RESTORE_TOPIC, restored);
    GenericRecord address = (GenericRecord) record.get(FIELD_SHIP_TO);
    assertEquals(CITY_VALUE, address.get(FIELD_CITY).toString());
    assertEquals(CODE_VALUE,
        ((GenericRecord) address.get(FIELD_CODE)).get(FIELD_CODE).toString());
  }

  @Test
  public void testAvroSharedReferenceRegisteredOnceAcrossSchemas() throws Exception {
    byte[] order = avroRecordBytes(SOURCE_TOPIC, ORDER_SCHEMA);
    byte[] invoice = avroRecordBytes(SECOND_SOURCE_TOPIC, INVOICE_SCHEMA);
    SchemaBackupAwareByteArrayConverter backup = newConverter(source, false);
    SchemaBackupAwareByteArrayConverter restore = newConverter(target, false);

    restore(backup, restore, SOURCE_TOPIC, RESTORE_TOPIC, order);
    restore(backup, restore, SECOND_SOURCE_TOPIC, SECOND_RESTORE_TOPIC, invoice);

    assertEquals(1, Collections.frequency(target.registeredSubjects, ADDRESS_SUBJECT));
    assertEquals(1, Collections.frequency(target.registeredSubjects, COUNTRY_SUBJECT));
    assertTrue(target.registeredSubjects.contains(SECOND_RESTORE_TOPIC + VALUE_SUFFIX));
  }

  @Test
  public void testProtobufImportReferenceRestores() throws Exception {
    source.register(MONEY_SUBJECT, new ProtobufSchema(MONEY_PROTO));
    ProtobufSchema payment = new ProtobufSchema(PAYMENT_PROTO,
        Collections.singletonList(
            new SchemaReference(MONEY_PROTO_NAME, MONEY_SUBJECT, FIRST_VERSION)),
        Collections.singletonMap(MONEY_PROTO_NAME, MONEY_PROTO), null, null);
    int id = source.register(SOURCE_TOPIC + VALUE_SUFFIX, payment);
    source.registeredSubjects.clear();

    Descriptor paymentDescriptor = payment.toDescriptor();
    Descriptor moneyDescriptor = paymentDescriptor.findFieldByName(FIELD_AMOUNT).getMessageType();
    DynamicMessage money = DynamicMessage.newBuilder(moneyDescriptor)
        .setField(moneyDescriptor.findFieldByName(FIELD_CURRENCY), CURRENCY_VALUE)
        .setField(moneyDescriptor.findFieldByName(FIELD_UNITS), UNITS_VALUE)
        .build();
    DynamicMessage message = DynamicMessage.newBuilder(paymentDescriptor)
        .setField(paymentDescriptor.findFieldByName(FIELD_ID), ID_VALUE)
        .setField(paymentDescriptor.findFieldByName(FIELD_AMOUNT), money)
        .build();
    byte[] original = prefixed(id, concat(new byte[] {FIRST_MESSAGE_INDEX}, message.toByteArray()));

    byte[] restored = restore(newConverter(source, false), newConverter(target, false),
        SOURCE_TOPIC, RESTORE_TOPIC, original);

    assertEquals(Arrays.asList(MONEY_SUBJECT, RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
    assertPayloadUnchanged(original, restored);
    KafkaProtobufDeserializer<DynamicMessage> deserializer =
        new KafkaProtobufDeserializer<>(target);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    DynamicMessage decoded = deserializer.deserialize(RESTORE_TOPIC, restored);
    Descriptor decodedDescriptor = decoded.getDescriptorForType();
    assertEquals(ID_VALUE, decoded.getField(decodedDescriptor.findFieldByName(FIELD_ID)));
    DynamicMessage decodedMoney =
        (DynamicMessage) decoded.getField(decodedDescriptor.findFieldByName(FIELD_AMOUNT));
    assertEquals(UNITS_VALUE, decodedMoney.getField(
        decodedMoney.getDescriptorForType().findFieldByName(FIELD_UNITS)));
  }

  @Test
  public void testJsonSchemaRefReferenceRestores() throws Exception {
    source.register(ADDRESS_JSON_SUBJECT, new JsonSchema(ADDRESS_JSON_SCHEMA));
    JsonSchema shipment = new JsonSchema(SHIPMENT_JSON_SCHEMA,
        Collections.singletonList(
            new SchemaReference(ADDRESS_JSON_NAME, ADDRESS_JSON_SUBJECT, FIRST_VERSION)),
        Collections.singletonMap(ADDRESS_JSON_NAME, ADDRESS_JSON_SCHEMA), null);
    int id = source.register(SOURCE_TOPIC + VALUE_SUFFIX, shipment);
    source.registeredSubjects.clear();
    byte[] original = prefixed(id, SHIPMENT_JSON.getBytes(StandardCharsets.UTF_8));

    byte[] restored = restore(newConverter(source, false), newConverter(target, false),
        SOURCE_TOPIC, RESTORE_TOPIC, original);

    assertEquals(Arrays.asList(ADDRESS_JSON_SUBJECT, RESTORE_TOPIC + VALUE_SUFFIX),
        target.registeredSubjects);
    assertPayloadUnchanged(original, restored);
    KafkaJsonSchemaDeserializer<Object> deserializer = new KafkaJsonSchemaDeserializer<>(target);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    assertTrue(String.valueOf(deserializer.deserialize(RESTORE_TOPIC, restored))
        .contains(CITY_VALUE));
  }

  // ---------------- Key side ----------------

  @Test
  public void testKeyConverterUsesKeySubject() throws Exception {
    AvroSchema keySchema = new AvroSchema(COUNTRY_SCHEMA);
    int id = source.register(SOURCE_TOPIC + KEY_SUFFIX, keySchema);
    GenericRecord key = new GenericData.Record(keySchema.rawSchema());
    key.put(FIELD_CODE, CODE_VALUE);
    byte[] original = prefixed(id, encode(keySchema.rawSchema(), key));

    byte[] restored = restore(newConverter(source, true), newConverter(target, true),
        SOURCE_TOPIC, RESTORE_TOPIC, original);

    assertEquals(Collections.singletonList(RESTORE_TOPIC + KEY_SUFFIX), target.registeredSubjects);
    assertPayloadUnchanged(original, restored);
  }

  @Test
  public void testKeyTombstoneRoundTrips() {
    SchemaBackupAwareByteArrayConverter keyConverter = newConverter(source, true);
    SchemaAndValue wrapped = keyConverter.toConnectData(SOURCE_TOPIC, null);

    assertEquals(SchemaAndValue.NULL, wrapped);
    assertNull(newConverter(target, true).fromConnectData(RESTORE_TOPIC, null, null));
    assertTrue(target.registeredSubjects.isEmpty());
  }

  // ---------------- Helpers ----------------

  private static String recordWithAddress(String name) {
    return "{\"type\":\"record\",\"name\":\"" + name + "\",\"namespace\":\"" + NAMESPACE + "\","
        + "\"fields\":["
        + "{\"name\":\"" + FIELD_ID + "\",\"type\":\"string\"},"
        + "{\"name\":\"" + FIELD_SHIP_TO + "\",\"type\":\"" + ADDRESS_NAME + "\"}"
        + "]}";
  }

  /** Registers Country, Address and the given record on the source; returns wire bytes. */
  private byte[] avroRecordBytes(String topic, String recordSchema) throws Exception {
    source.register(COUNTRY_SUBJECT, new AvroSchema(COUNTRY_SCHEMA));
    AvroSchema address = new AvroSchema(ADDRESS_SCHEMA,
        Collections.singletonList(new SchemaReference(COUNTRY_NAME, COUNTRY_SUBJECT, FIRST_VERSION)),
        Collections.singletonMap(COUNTRY_NAME, COUNTRY_SCHEMA), null);
    source.register(ADDRESS_SUBJECT, address);

    Map<String, String> resolved = new LinkedHashMap<>();
    resolved.put(COUNTRY_NAME, COUNTRY_SCHEMA);
    resolved.put(ADDRESS_NAME, ADDRESS_SCHEMA);
    AvroSchema schema = new AvroSchema(recordSchema,
        Collections.singletonList(new SchemaReference(ADDRESS_NAME, ADDRESS_SUBJECT, FIRST_VERSION)),
        resolved, null);
    int id = source.register(topic + VALUE_SUFFIX, schema);
    source.registeredSubjects.clear();

    org.apache.avro.Schema avro = schema.rawSchema();
    org.apache.avro.Schema addressAvro = avro.getField(FIELD_SHIP_TO).schema();
    GenericRecord country = new GenericData.Record(addressAvro.getField(FIELD_CODE).schema());
    country.put(FIELD_CODE, CODE_VALUE);
    GenericRecord addressRecord = new GenericData.Record(addressAvro);
    addressRecord.put(FIELD_CITY, CITY_VALUE);
    addressRecord.put(FIELD_CODE, country);
    GenericRecord record = new GenericData.Record(avro);
    record.put(FIELD_ID, ID_VALUE);
    record.put(FIELD_SHIP_TO, addressRecord);
    return prefixed(id, encode(avro, record));
  }

  private static byte[] restore(
      SchemaBackupAwareByteArrayConverter backup, SchemaBackupAwareByteArrayConverter restore,
      String sourceTopic, String restoreTopic, byte[] original) {
    SchemaAndValue wrapped = backup.toConnectData(sourceTopic, original);
    return restore.fromConnectData(
        restoreTopic, new RecordHeaders(), wrapped.schema(), wrapped.value());
  }

  private static SchemaBackupAwareByteArrayConverter newConverter(
      MockSchemaRegistryClient client, boolean isKey) {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put(SR_URL_CONFIG, SR_URL_VALUE);
    cfg.put(SchemaBackupConfig.SCHEMA_BACKUP_ENABLED_CONFIG, TRUE);
    SchemaBackupAwareByteArrayConverter converter = new SchemaBackupAwareByteArrayConverter(client);
    converter.configure(cfg, isKey);
    return converter;
  }

  private KafkaAvroDeserializer avroDeserializer() {
    KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(target);
    deserializer.configure(Collections.singletonMap(SR_URL_CONFIG, SR_URL_VALUE), false);
    return deserializer;
  }

  private static byte[] encode(org.apache.avro.Schema schema, GenericRecord record)
      throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
    new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  private static byte[] prefixed(int id, byte[] payload) {
    return ByteBuffer.allocate(PREFIX_LENGTH + payload.length)
        .put((byte) 0).putInt(id).put(payload).array();
  }

  private static byte[] concat(byte[] first, byte[] second) {
    return ByteBuffer.allocate(first.length + second.length).put(first).put(second).array();
  }

  private static void assertPayloadUnchanged(byte[] original, byte[] restored) {
    assertArrayEquals(
        Arrays.copyOfRange(original, PREFIX_LENGTH, original.length),
        Arrays.copyOfRange(restored, PREFIX_LENGTH, restored.length));
  }

  /** Records the subjects registered on this client, in order. */
  private static class RecordingClient extends MockSchemaRegistryClient {
    final List<String> registeredSubjects = new ArrayList<>();

    RecordingClient() {
      super(Arrays.<SchemaProvider>asList(
          new AvroSchemaProvider(), new ProtobufSchemaProvider(), new JsonSchemaProvider()));
    }

    @Override
    public int register(String subject, ParsedSchema schema)
        throws IOException, RestClientException {
      registeredSubjects.add(subject);
      return super.register(subject, schema);
    }

    @Override
    public RegisterSchemaResponse registerWithResponse(String subject, ParsedSchema schema, boolean normalize,
            boolean propagateSchemaTags) throws IOException, RestClientException {
      registeredSubjects.add(subject);
      return super.registerWithResponse(subject, schema, normalize, propagateSchemaTags);
    }
  }
}
