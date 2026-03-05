/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.streams.unit;

import io.confluent.kafka.serializers.schema.id.SchemaId;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.test.TestRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class Utils {
    /**
     * Verify that the stored record has both __key_schema_id and __value_schema_id headers are present,
     * with V1 magic byte (GUID format, 17 bytes)
     */
    public static void assertSchemaIdHeadersOnRecord(TestRecord<GenericRecord, GenericRecord> record, String context) {
        assertNotNull(record.headers(), context + ": headers should not be null");

        Header keyHeader = record.headers().lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keyHeader, context + ": should have __key_schema_id header");
        assertEquals(17, keyHeader.value().length, context + ": key GUID should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeader.value()[0],
            context + ": key header should have V1 magic byte");

        Header valueHeader = record.headers().lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueHeader, context + ": should have __value_schema_id header");
        assertEquals(17, valueHeader.value().length, context + ": value GUID should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, valueHeader.value()[0],
            context + ": value header should have V1 magic byte");
    }
}
