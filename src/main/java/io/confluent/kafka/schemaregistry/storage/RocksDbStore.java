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
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

public class RocksDbStore implements Store<byte[], byte[]> {

  private final Options options;
  private final File rocksDbDir;
  private final AtomicBoolean initialized = new AtomicBoolean(false);
  private RocksDB db;

  public RocksDbStore(RocksDbConfig config) {
    rocksDbDir = new File(config.getString(RocksDbConfig.ROCKSDB_DATADIR_CONFIG));
    options = new Options();
    String compression = config.getString(RocksDbConfig.ROCKSDB_COMPRESSION_CONFIG);
    CompressionType compressionType = CompressionType.valueOf(compression);
    options.setCompressionType(compressionType);
    String compaction = config.getString(RocksDbConfig.ROCKSDB_COMPACTION_STYLE_CONFIG);
    CompactionStyle compactionStyle = CompactionStyle.valueOf(compaction);
    //    options.setCompactionStyle(compactionStyle);
    //    options.setWriteBufferSize(config.getLong(RocksDbConfig.ROCKSDB_WRITE_BUFFER_SIZE_BYTES_CONFIG));
    //    options.setMaxWriteBufferNumber(config.getInt(RocksDbConfig.ROCKSDB_WRITE_BUFFERS_CONFIG));
    //    options.setErrorIfExists(true);
    options.setCreateIfMissing(true);
  }

  @Override
  public void init() throws StoreInitializationException {
    try {
      db = RocksDB.open(options, rocksDbDir.getAbsolutePath());
    } catch (RocksDBException e) {
      throw new StoreInitializationException("Error while initializing the RocksDB database", e);
    }
    boolean isInitialized = initialized.compareAndSet(false, true);
    if (!isInitialized) {
      throw new StoreInitializationException("Illegal state while initializing store. Store " +
                                             "was already initialized");
    }
  }

  @Override
  public byte[] get(byte[] key) throws StoreException {
    assertInitialized();
    if (key == null) {
      throw new IllegalArgumentException("Key for a RocksDB get operation must not be null");
    }
    byte[] value = null;
    try {
      value = db.get(key);
    } catch (RocksDBException e) {
      throw new StoreException(e);
    }
    return value;
  }

  @Override
  public void put(byte[] key, byte[] value) throws StoreException {
    assertInitialized();
    if (key == null) {
      throw new IllegalArgumentException("Key for a RocksDB put operation must not be null");
    }
    if (value == null) {
      throw new IllegalArgumentException("Value for a RocksDB put operation must not be null");
    }
    try {
      db.put(key, value);
    } catch (RocksDBException e) {
      throw new StoreException("Error during a RocksDB put operation", e);
    }
  }

  @Override
  public Iterator<byte[]> getAll(byte[] key1, byte[] key2) throws StoreException {
    // TODO Auto-generated method stub
    assertInitialized();
//        if (key1 == null && key2 == null) {
//            RocksIterator iterator = db.newIterator();
//            iterator.seekToFirst();
//            return iterator;
//        }
    return null;
  }

  @Override
  public void putAll(Map<byte[], byte[]> entries) throws StoreException {
    // TODO Auto-generated method stub
    assertInitialized();

  }

  @Override
  public void delete(byte[] key) throws StoreException {
    assertInitialized();
    try {
      db.put(key, null);
    } catch (RocksDBException e) {
      throw new StoreException("Error during a RocksDB delete operation", e);
    }
  }

  @Override
  public Iterator<byte[]> getAllKeys() throws StoreException {
    //TODO: to be implemented
    throw new UnsupportedOperationException("To be implemented");
  }

  @Override
  public void close() {
    if (db != null) {
      db.close();
    }
    // TODO: Also delete the data directory
  }

  private void assertInitialized() throws StoreException {
    if (!initialized.get()) {
      throw new StoreException("Illegal state. Store not initialized yet");
    }
  }
}
