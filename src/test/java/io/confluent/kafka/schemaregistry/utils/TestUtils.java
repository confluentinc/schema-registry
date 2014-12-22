package io.confluent.kafka.schemaregistry.utils;

import java.io.File;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;

import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaSerializer;

import static org.junit.Assert.fail;

public class TestUtils {

  static String IoTmpDir = System.getProperty("java.io.tmpdir");
  static Random random = new Random();
  static SchemaSerializer schemaSerializer = new SchemaSerializer();

  /**
   * Create a temporary directory
   */
  public static File tempDir(String namePrefix) {
    final File f = new File(IoTmpDir, namePrefix + "-" + random.nextInt(1000000));
    f.mkdirs();
    f.deleteOnExit();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        rm(f);
      }
    });
    return f;
  }

  /**
   * Recursively delete the given file/directory and any subfiles (if any exist)
   *
   * @param file The root file at which to begin deleting
   */
  public static void rm(File file) {
    if (file == null) {
      return;
    } else if (file.isDirectory()) {
      File[] files = file.listFiles();
      if (files != null) {
        for (File f : files) {
          rm(f);
        }
      }
    } else {
      file.delete();
    }
  }

  /**
   * Create a KafkaSchemaRegistry instance and initialize it.
   * @param props
   * @return
   */
  public static KafkaSchemaRegistry createAndInitSchemaRegistryInstance(Properties props) {
    KafkaSchemaRegistry schemaRegistry = null;
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(props);
      schemaRegistry = new KafkaSchemaRegistry(schemaRegistryConfig, schemaSerializer);
      schemaRegistry.init();
    } catch (SchemaRegistryException e) {
      fail("Can't instantiate KafkaSchemaRegistry: " + e);
    }
    return schemaRegistry;
  }

  /**
   * Wait until a callable returns true or the timeout is reached.
   * @param callable
   * @param timeoutMs
   * @param errorMsg
   * @throws Exception
   */
  public static void waitUntilTrue(Callable<Boolean> callable, long timeoutMs, String errorMsg) {
    try {
    long startTime = System.currentTimeMillis();
      do {
        Boolean state = callable.call();
        if (state)
          return;
        if (System.currentTimeMillis() > startTime + timeoutMs) {
          fail(errorMsg);
        }
        Thread.sleep(50);
      } while (true);
    } catch (Exception e) {
      fail("Unexpected exception: " + e);
    }
  }
}
