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
package io.confluent.kafka.schemaregistry.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class OffsetCheckpointTest {

    private final String topic = "topic";

    @Test
    public void testReadWrite() throws IOException {
        try (final OffsetCheckpoint checkpoint = new OffsetCheckpoint("/tmp", 0, topic)) {
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), 0L);
            offsets.put(new TopicPartition(topic, 1), 1L);
            offsets.put(new TopicPartition(topic, 2), 2L);

            checkpoint.write(offsets);
            assertEquals(offsets, checkpoint.read());

            checkpoint.delete();
            assertFalse(new File("/tmp", OffsetCheckpoint.CHECKPOINT_FILE_NAME).exists());

            offsets.put(new TopicPartition(topic, 3), 3L);
            checkpoint.write(offsets);
            assertEquals(offsets, checkpoint.read());

            checkpoint.delete();
        }
    }

    @Test
    public void shouldNotWriteCheckpointWhenNoOffsets() throws IOException {
        // we do not need to worry about file name uniqueness since this file should not be created
        try (final OffsetCheckpoint checkpoint = new OffsetCheckpoint("/tmp", 0, topic)) {

            checkpoint.write(Collections.<TopicPartition, Long>emptyMap());

            assertFalse(new File("/tmp", OffsetCheckpoint.CHECKPOINT_FILE_NAME).exists());

            assertEquals(Collections.<TopicPartition, Long>emptyMap(), checkpoint.read());

            // deleting a non-exist checkpoint file should be fine
            checkpoint.delete();
        }
    }

    @Test
    public void testReadOnly() throws Exception {
        try (final OffsetCheckpoint checkpoint = new OffsetCheckpoint("/tmp", 0, topic)) {
            final Map<TopicPartition, Long> offsets = new HashMap<>();
            offsets.put(new TopicPartition(topic, 0), 0L);
            offsets.put(new TopicPartition(topic, 1), 1L);
            offsets.put(new TopicPartition(topic, 2), 2L);

            checkpoint.write(offsets);
            assertEquals(offsets, checkpoint.read());

            try (final OffsetCheckpointReadOnly checkpointReadOnly =
                     new OffsetCheckpointReadOnly("/tmp", 0, topic)) {
                assertEquals(offsets, checkpointReadOnly.read());
                assertThrows(UnsupportedOperationException.class,
                    () -> checkpointReadOnly.write(offsets));
            }
            checkpoint.delete();
        }
    }
}
