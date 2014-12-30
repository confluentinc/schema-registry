/**
 * Copyright 2014 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.storage;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.confluent.common.config.AbstractConfig;
import io.confluent.common.config.ConfigDef;
import io.confluent.common.config.ConfigDef.Importance;
import io.confluent.common.config.ConfigDef.Type;

import static io.confluent.common.config.ConfigDef.Range.atLeast;

public class RocksDbConfig extends AbstractConfig {

  /**
   * <code>rocksdb.data.dir</code>
   */
  public static final String ROCKSDB_DATADIR_CONFIG = "rocksdb.data.dir";
  /**
   * <code>rocksdb.compression</code>
   */
  public static final String ROCKSDB_COMPRESSION_CONFIG = "rocksdb.compression";
  /**
   * <code>rocksdb.block.size.bytes</code>
   */
  public static final String ROCKSDB_BLOCK_SIZE_BYTES_CONFIG = "rocksdb.block.size.bytes";
  /**
   * <code>rocksdb.write.buffer.size.bytes</code>
   */
  public static final String ROCKSDB_WRITE_BUFFER_SIZE_BYTES_CONFIG =
      "rocksdb.write.buffer.size.bytes";
  /**
   * <code>rocksdb.bloomfilter.bits</code>
   */
  public static final String ROCKSDB_BLOOMFILTER_BITS_CONFIG = "rocksdb.bloomfilter.bits";
  public static final String ROCKSDB_COMPACTION_STYLE_CONFIG = "rocksdb.compaction.style";
  public static final String ROCKSDB_WRITE_BUFFERS_CONFIG = "rocksdb.write.buffers";
  private static final String ROCKSDB_DATADIR_DOC = "Location of the rocksdb data";
  private static final String ROCKSDB_COMPRESSION_DOC =
      "The compression setting and choice for data stored in RocksDB. Could be one of" +
      "snappy, bzip2, zlib, lz4, lz4hc, none";
  private static final List<String> compressionOptions =
      Arrays.asList("snappy", "bzip2", "zlib", "lz4", "lz4hc", "none");
  private static final String ROCKSDB_BLOCK_SIZE_DOC = "Block size in bytes for data in RocksDB";
  private static final String ROCKSDB_WRITE_BUFFER_SIZE_BYTES_DOC =
      "Write buffer size in bytes for data in RocksDB";
  private static final ConfigDef config = new ConfigDef()
      .define(ROCKSDB_COMPRESSION_CONFIG, Type.STRING, CompressionType.NO_COMPRESSION.toString(),
              Importance.MEDIUM, ROCKSDB_COMPRESSION_DOC)
      .define(ROCKSDB_DATADIR_CONFIG, Type.STRING, Importance.HIGH, ROCKSDB_DATADIR_DOC)
      .define(ROCKSDB_COMPACTION_STYLE_CONFIG, Type.STRING, CompactionStyle.UNIVERSAL.toString(),
              Importance.MEDIUM,
              ROCKSDB_COMPACTION_STYLE_DOC)
      .define(ROCKSDB_BLOCK_SIZE_BYTES_CONFIG, Type.INT, 4096, Importance.MEDIUM,
              ROCKSDB_BLOCK_SIZE_DOC)
      .define(ROCKSDB_BLOOMFILTER_BITS_CONFIG, Type.INT, 10, atLeast(0), Importance.LOW, null)
      .define(ROCKSDB_WRITE_BUFFERS_CONFIG, Type.INT, 1, Importance.LOW, null)
      .define(ROCKSDB_WRITE_BUFFER_SIZE_BYTES_CONFIG, Type.LONG, 4096, Importance.MEDIUM,
              ROCKSDB_WRITE_BUFFER_SIZE_BYTES_DOC);
  private static final String ROCKSDB_COMPACTION_STYLE_DOC =
      "The compaction style for RocksDB. Could be one of universal, fifo, level";
  private static final List<String> compactionOptions =
      Arrays.asList("universal", "level", "fifo");

  public RocksDbConfig(ConfigDef arg0, Map<?, ?> arg1) {
    super(arg0, arg1);
  }

  RocksDbConfig(Map<? extends Object, ? extends Object> props) {
    super(config, props);
  }

  public static void main(String[] args) {
    System.out.println(config.toHtmlTable());
  }

}
