/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.tink;

import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.daead.DeterministicAeadConfig;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

public class KmsDriverManager {

  static {
    try {
      AeadConfig.register();
      DeterministicAeadConfig.register();
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private static final List<KmsDriver> drivers = loadDrivers();

  private KmsDriverManager() {
  }

  public static KmsDriver getDriver(String keyUri)
      throws GeneralSecurityException {
    for (KmsDriver driver : drivers) {
      if (driver.doesSupport(keyUri)) {
        return driver;
      }
    }
    throw new GeneralSecurityException("No KMS driver supports: " + keyUri);
  }

  private static List<KmsDriver> loadDrivers() {
    List<KmsDriver> drivers = new ArrayList<>();
    ServiceLoader<KmsDriver> clientLoader = ServiceLoader.load(KmsDriver.class);
    for (KmsDriver element : clientLoader) {
      drivers.add(element);
    }
    return Collections.unmodifiableList(drivers);
  }
}

