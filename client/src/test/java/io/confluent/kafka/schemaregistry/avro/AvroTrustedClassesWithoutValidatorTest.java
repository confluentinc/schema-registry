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

package io.confluent.kafka.schemaregistry.avro;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import org.junit.Test;

// Avro before 1.12.2 has no ClassSecurityValidator, which this hides from a separate class loader
public class AvroTrustedClassesWithoutValidatorTest {

  private static final String VALIDATOR = "org.apache.avro.util.ClassSecurityValidator";

  @Test
  public void declaringTrustIsANoOpWithoutTheValidator() throws Exception {
    try (URLClassLoader hidden = new HidingClassLoader(VALIDATOR)) {
      Class<?> trusted = hidden.loadClass(AvroTrustedClasses.class.getName());
      assertSame(hidden, trusted.getClassLoader());
      trusted.getMethod("trust", Class[].class)
          .invoke(null, (Object) new Class<?>[] {String.class});
      trusted.getMethod("trustPackages", String[].class)
          .invoke(null, (Object) new String[] {"com.example"});
      trusted.getMethod("trustClassOf", Object.class).invoke(null, new Object());
      Method isInstalled = trusted.getMethod("isInstalled");
      assertFalse((Boolean) isInstalled.invoke(null));
    }
  }

  // Loads from the test class path, reporting the hidden classes as absent
  private static final class HidingClassLoader extends URLClassLoader {
    private final String hiddenPrefix;

    HidingClassLoader(String hiddenPrefix) throws MalformedURLException {
      super(classPath(), ClassLoader.getPlatformClassLoader());
      this.hiddenPrefix = hiddenPrefix;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith(hiddenPrefix)) {
        throw new ClassNotFoundException(name + " is hidden by this test");
      }
      return super.loadClass(name, resolve);
    }

    private static URL[] classPath() throws MalformedURLException {
      String classPath = System.getProperty("surefire.test.class.path",
          System.getProperty("java.class.path"));
      String[] entries = classPath.split(File.pathSeparator);
      URL[] urls = new URL[entries.length];
      for (int i = 0; i < entries.length; i++) {
        urls[i] = new File(entries[i]).toURI().toURL();
      }
      return urls;
    }
  }
}
