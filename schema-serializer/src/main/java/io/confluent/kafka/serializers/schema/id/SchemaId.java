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

package io.confluent.kafka.serializers.schema.id;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.utils.ByteUtils;

/**
 * A {@link SchemaId} is used to identify a schema. It can be either an ID or a GUID.
 */
public class SchemaId {
  public static final String KEY_SCHEMA_ID_HEADER = "__key_schema_id";
  public static final String VALUE_SCHEMA_ID_HEADER = "__value_schema_id";
  public static final int ID_SIZE = 4;
  public static final byte MAGIC_BYTE_V0 = 0x0;
  public static final byte MAGIC_BYTE_V1 = 0x1;

  private final String schemaType;
  private Integer id;
  private UUID guid;
  private SchemaMessageIndexes messageIndexes;

  public SchemaId(String schemaType) {
    this.schemaType = schemaType;
  }

  public SchemaId(String schemaType, Integer id, UUID guid) {
    this.schemaType = schemaType;
    this.id = id;
    this.guid = guid;
  }

  public SchemaId(String schemaType, Integer id, String guid) {
    this.schemaType = schemaType;
    this.id = id;
    this.guid = guid != null ? UUID.fromString(guid) : null;
  }

  public ByteBuffer fromBytes(ByteBuffer buffer) {
    byte version = buffer.get();
    if (version == MAGIC_BYTE_V0) {
      int id = buffer.getInt();
      setId(id);
    } else if (version == MAGIC_BYTE_V1) {
      long msb = buffer.getLong();
      long lsb = buffer.getLong();
      setGuid(new UUID(msb, lsb));
    } else {
      throw new IllegalArgumentException("Unknown magic byte!");
    }
    if (getSchemaType().equals("PROTOBUF")) {
      setMessageIndexes(buffer);
    }
    return buffer;
  }

  public byte[] idToBytes() throws SerializationException {
    if (id == null) {
      throw new SerializationException("Schema ID is null");
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      baos.write(MAGIC_BYTE_V0);
      baos.write(ByteBuffer.allocate(ID_SIZE).putInt(getId()).array());
      if (!getMessageIndexes().isEmpty()) {
        byte[] indexes = getMessageIndexesAsBytes();
        baos.write(indexes);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new SerializationException("Error serializing schema ID", e);
    }
  }

  public byte[] guidToBytes() throws SerializationException {
    if (guid == null) {
      throw new SerializationException("Schema GUID is null");
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      baos.write(MAGIC_BYTE_V1);
      ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
      bb.putLong(guid.getMostSignificantBits());
      bb.putLong(guid.getLeastSignificantBits());
      baos.write(bb.array());
      if (!getMessageIndexes().isEmpty()) {
        byte[] indexes = getMessageIndexesAsBytes();
        baos.write(indexes);
      }
      return baos.toByteArray();
    } catch (Exception e) {
      throw new SerializationException("Error serializing schema GUID", e);
    }
  }

  public String getSchemaType() {
    return schemaType;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public UUID getGuid() {
    return guid;
  }

  public void setGuid(UUID guid) {
    this.guid = guid;
  }

  public List<Integer> getMessageIndexes() {
    return messageIndexes != null ? messageIndexes.indexes() : Collections.emptyList();
  }

  public byte[] getMessageIndexesAsBytes() {
    return messageIndexes != null ? messageIndexes.toByteArray() : new byte[0];
  }

  public void setMessageIndexes(List<Integer> messageIndexes) {
    this.messageIndexes = new SchemaMessageIndexes(messageIndexes);
  }

  public void setMessageIndexes(byte[] bytes) {
    this.messageIndexes = SchemaMessageIndexes.readFrom(bytes);
  }

  public void setMessageIndexes(ByteBuffer buffer) {
    this.messageIndexes = SchemaMessageIndexes.readFrom(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaId schemaId = (SchemaId) o;
    return Objects.equals(schemaType, schemaId.schemaType) && Objects.equals(id,
        schemaId.id) && Objects.equals(guid, schemaId.guid) && Objects.equals(
        messageIndexes, schemaId.messageIndexes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(schemaType, id, guid, messageIndexes);
  }

  @Override
  public String toString() {
    return "SchemaId{"
        + "schemaType='" + schemaType + '\''
        + ", id=" + id
        + ", guid='" + guid + '\''
        + ", messageIndexes=" + messageIndexes
        + '}';
  }

  static class SchemaMessageIndexes {

    private static final List<Integer> DEFAULT_INDEX = Collections.singletonList(0);
    private static final byte[] DEFAULT_MSG_INDEXES = new byte[]{0x00};

    private final List<Integer> indexes;

    public SchemaMessageIndexes(List<Integer> indexes) {
      this.indexes = indexes;
    }

    public List<Integer> indexes() {
      return indexes;
    }

    public byte[] toByteArray() {
      if (indexes.equals(DEFAULT_INDEX)) {
        return DEFAULT_MSG_INDEXES;
      }
      int size = ByteUtils.sizeOfVarint(indexes.size());
      for (Integer index : indexes) {
        size += ByteUtils.sizeOfVarint(index);
      }
      ByteBuffer buffer = ByteBuffer.allocate(size);
      writeTo(buffer);
      return buffer.array();
    }

    public void writeTo(ByteBuffer buffer) {
      ByteUtils.writeVarint(indexes.size(), buffer);
      for (Integer index : indexes) {
        ByteUtils.writeVarint(index, buffer);
      }
    }

    public static SchemaMessageIndexes readFrom(byte[] bytes) {
      return readFrom(ByteBuffer.wrap(bytes));
    }

    public static SchemaMessageIndexes readFrom(ByteBuffer buffer) {
      int size = ByteUtils.readVarint(buffer);
      if (size == 0) {
        // optimization
        return new SchemaMessageIndexes(DEFAULT_INDEX);
      }
      List<Integer> indexes = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        indexes.add(ByteUtils.readVarint(buffer));
      }
      return new SchemaMessageIndexes(indexes);
    }
  }
}

