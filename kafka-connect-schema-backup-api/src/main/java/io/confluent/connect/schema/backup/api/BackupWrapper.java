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

package io.confluent.connect.schema.backup.api;

import io.confluent.kafka.serializers.schema.id.SchemaId;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

/**
 * Backup Wrapper struct for carrying schema metadata between converters and the envelope layer.
 */
public final class BackupWrapper {

  public static final String NAME = "io.confluent.connect.backup.Wrapper";

  public static final String FIELD_DATA = "data";
  public static final String FIELD_SCHEMA_ID = "schemaId";
  public static final String FIELD_SCHEMA_VERSION = "schemaVersion";
  public static final String FIELD_SCHEMA_TYPE = "schemaType";
  public static final String FIELD_SCHEMA_SUBJECT = "schemaSubject";
  public static final String FIELD_RAW_SCHEMA = "rawSchema";
  public static final String FIELD_REFERENCE_TREE = "referenceTree";
  public static final String FIELD_DIRECT_REFS = "directRefs";
  public static final String FIELD_SCHEMA_GUID = "schemaGuid";

  private BackupWrapper() {
  }

  /**
   * Groups metadata fields for {@link #buildWrapper} to reduce parameter count.
   */
  public static class WrapperFields {
    private final Integer schemaId;
    private final Integer schemaVersion;
    private final String schemaType;
    private final String subject;
    private final String rawSchema;
    private final String referenceTreeJson;
    private final String directRefsJson;
    private final String schemaGuid;

    public WrapperFields(Integer schemaId, Integer schemaVersion,
        String schemaType, String subject, String rawSchema,
        String referenceTreeJson, String directRefsJson) {
      this(schemaId, schemaVersion, schemaType, subject, rawSchema,
          referenceTreeJson, directRefsJson, null);
    }

    public WrapperFields(Integer schemaId, Integer schemaVersion,
        String schemaType, String subject, String rawSchema,
        String referenceTreeJson, String directRefsJson,
        String schemaGuid) {
      this.schemaId = schemaId;
      this.schemaVersion = schemaVersion;
      this.schemaType = schemaType;
      this.subject = subject;
      this.rawSchema = rawSchema;
      this.referenceTreeJson = referenceTreeJson;
      this.directRefsJson = directRefsJson;
      this.schemaGuid = schemaGuid;
    }

    public Integer getSchemaId() {
      return schemaId;
    }

    public Integer getSchemaVersion() {
      return schemaVersion;
    }

    public String getSchemaType() {
      return schemaType;
    }

    public String getSubject() {
      return subject;
    }

    public String getRawSchema() {
      return rawSchema;
    }

    public String getReferenceTreeJson() {
      return referenceTreeJson;
    }

    public String getDirectRefsJson() {
      return directRefsJson;
    }

    public String getSchemaGuid() {
      return schemaGuid;
    }
  }

  public static Schema buildSchema(Schema dataSchema) {
    Schema safeDataSchema = dataSchema != null
        ? dataSchema : Schema.OPTIONAL_BYTES_SCHEMA;
    return SchemaBuilder.struct()
        .name(NAME)
        .field(FIELD_DATA, safeDataSchema)
        .field(FIELD_SCHEMA_ID, Schema.OPTIONAL_INT32_SCHEMA)
        .field(FIELD_SCHEMA_VERSION, Schema.OPTIONAL_INT32_SCHEMA)
        .field(FIELD_SCHEMA_GUID, Schema.OPTIONAL_STRING_SCHEMA)
        .field(FIELD_SCHEMA_TYPE, Schema.STRING_SCHEMA)
        .field(FIELD_SCHEMA_SUBJECT, Schema.STRING_SCHEMA)
        .field(FIELD_RAW_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
        .field(FIELD_REFERENCE_TREE, Schema.OPTIONAL_STRING_SCHEMA)
        .field(FIELD_DIRECT_REFS, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }

  public static Struct buildWrapper(
      Schema wrapperSchema, Object data, WrapperFields fields) {
    Struct wrapper = new Struct(wrapperSchema);
    wrapper.put(FIELD_DATA, data);
    wrapper.put(FIELD_SCHEMA_ID, fields.getSchemaId());
    wrapper.put(FIELD_SCHEMA_VERSION, fields.getSchemaVersion());
    wrapper.put(FIELD_SCHEMA_TYPE, fields.getSchemaType());
    wrapper.put(FIELD_SCHEMA_SUBJECT, fields.getSubject());
    wrapper.put(FIELD_RAW_SCHEMA, fields.getRawSchema());
    wrapper.put(FIELD_REFERENCE_TREE, fields.getReferenceTreeJson());
    wrapper.put(FIELD_DIRECT_REFS, fields.getDirectRefsJson());
    wrapper.put(FIELD_SCHEMA_GUID, fields.getSchemaGuid());
    return wrapper;
  }

  public static boolean isWrapper(Schema schema) {
    return schema != null && NAME.equals(schema.name());
  }

  public static void removeSchemaIdHeaders(Headers headers, boolean isKey) {
    if (headers != null) {
      headers.remove(isKey
          ? SchemaId.KEY_SCHEMA_ID_HEADER : SchemaId.VALUE_SCHEMA_ID_HEADER);
    }
  }
}
