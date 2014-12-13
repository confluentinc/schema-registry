/**
 *
 */
package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * @author nnarkhed
 *
 */
public class RocksDbStore implements Store<byte[], byte[]> {

    private RocksDB db;
    private final Options options;
    private final File rocksDbDir;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

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

    @Override public void init() throws StoreInitializationException {
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
        if(key == null) {
            throw new IllegalArgumentException("Key for a RocksDB get operation must not be null");
        }
        byte[] value = null;
        try {
            value = db.get(key);
        } catch (RocksDBException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return value;
    }

    @Override
    public void put(byte[] key, byte[] value) throws StoreException {
        assertInitialized();
        if(key == null) {
            throw new IllegalArgumentException("Key for a RocksDB put operation must not be null");
        }
        if(value == null) {
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
    public void close() {
        if (db != null)
            db.close();
        // TODO: Also delete the data directory
    }

    private void assertInitialized() throws StoreException {
        if (!initialized.get()) {
            throw new StoreException("Illegal state. Store not initialized yet");
        }
    }
}
