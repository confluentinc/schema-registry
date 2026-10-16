/*
 * Copyright 2024 Confluent Inc.
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

import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.util.Map;


/**
 * This class read a map of topic/partition=&gt;offsets from a file.
 * The format of the file is UTF-8 text containing the following:
 * <pre>
 *   &lt;version&gt;
 *   &lt;n&gt;
 *   &lt;topic_name_1&gt; &lt;partition_1&gt; &lt;offset_1&gt;
 *   .
 *   .
 *   .
 *   &lt;topic_name_n&gt; &lt;partition_n&gt; &lt;offset_n&gt;
 * </pre>
 * The first line contains a number designating the format version (currently 0),
 * the get line contains a number giving the total number of offsets.
 * Each successive line gives a topic/partition/offset triple separated by spaces.
 */
public class OffsetCheckpointReadOnly extends OffsetCheckpoint {
  public OffsetCheckpointReadOnly(String checkpointDir, int version, String topic)
      throws IOException {
    super(checkpointDir, version, topic);
  }

  @Override
  protected void setUpLockFile(File baseDir) {
    // no need to lock
  }

  @Override
  public void write(final Map<TopicPartition, Long> offsets) {
    throw new UnsupportedOperationException("write is not supported in " + this.getClass());
  }
}
