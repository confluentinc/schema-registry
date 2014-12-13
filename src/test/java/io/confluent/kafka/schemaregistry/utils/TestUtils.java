package io.confluent.kafka.schemaregistry.utils;

import java.io.File;
import java.util.Random;

public class TestUtils {
  static String IoTmpDir = System.getProperty("java.io.tmpdir");
  static Random random = new Random();
  
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
   * @param file The root file at which to begin deleting
   */
  public static void rm(File file) {
    if(file == null) {
      return;
    } else if(file.isDirectory()) {
      File[] files = file.listFiles();
      if(files != null) {
        for(File f : files) {
          rm(f);
        }
      }
    } else {
      file.delete();
    }
  }
}
