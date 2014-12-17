package io.confluent.kafka.schemaregistry.storage;

public class RocksDbTest {

//  @Test
//  public void testIncorrectInitialization() {
//    Properties props = new Properties();
//    Store rocksDbStore = null;
//    try {
//      RocksDbConfig config = new RocksDbConfig(props);
//      rocksDbStore = new RocksDbStore(config);
//      fail("RocksDB store initialization should fail since required data directory is not configured");
//    } catch (ConfigException ce) {
//      // expected exception      
//    } catch (StoreInitializationException sie) {
//      // unexpected exception
//      fail("RocksDB store initialization should fail during configuration");
//    } finally {
//      // store is not initialized so don't need to close
//    }
//  }

//  @Test
//  public void testInitialization() {
//    Properties props = new Properties();
//    props.put(RocksDbConfig.ROCKSDB_DATADIR_CONFIG, TestUtils.tempDir("rocksdb").getAbsolutePath());
//    Store rocksDbStore = null;
//    try {
//      RocksDbConfig config = new RocksDbConfig(props);
//      rocksDbStore = new RocksDbStore(config);
//    } catch (ConfigException ce) {
//      ce.printStackTrace();
//      fail("RocksDB store initialization should not fail since required data directory is configured");
//    } catch (StoreInitializationException sie) {
//      sie.printStackTrace();
//      fail("RocksDB store initialization should not fail since required data directory is configured");
//    } finally {
//      if(rocksDbStore != null) {
//        rocksDbStore.close();
//      }
//    }
//  }

//  @Test
//  public void testPut() {
//    Properties props = new Properties();
//    props.put(RocksDbConfig.ROCKSDB_DATADIR_CONFIG, TestUtils.tempDir("rocksdb").getAbsolutePath());
//    Store db = null;
//    RocksDbConfig config = new RocksDbConfig(props);
//    try {
//      db = new RocksDbStore(config);
//    } catch (StoreInitializationException e) {
//      e.printStackTrace();
//      fail("RocksDB store initialization failed");
//    } finally {
//      if(db != null) {
//        db.close();
//      }      
//    }
//    byte[] value = null;
//    try {
//      db.put("Kafka".getBytes(), "rocks".getBytes());
//      value = db.get("Kafka".getBytes());
//    } catch (StoreException e) {
//      e.printStackTrace();
//      fail("RocksDB store put failed");
//    }
//    String deserializedValue = new String(value);
//    assertEquals("rocks", deserializedValue);
//    db.close();
//  }

//  @Test
//  public void testGetNonExistingValue() {
//    Properties props = new Properties();
//    props.put(RocksDbConfig.ROCKSDB_DATADIR_CONFIG, TestUtils.tempDir("rocksdb").getAbsolutePath());
//    Store db = null;
//    RocksDbConfig config = new RocksDbConfig(props);
//    try {
//      db = new RocksDbStore(config);
//    } catch (StoreInitializationException e) {
//      e.printStackTrace();
//      fail("RocksDB store initialization failed");
//    } finally {
//      if(db != null) {
//        db.close();
//      }
//    }
//    byte[] value = null;
//    value = db.get("Kafka".getBytes());
//    assertNull(value);
//    db.close();
//  }
}
