/*
 * Copyright 2023 Confluent Inc.
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

import com.google.crypto.tink.Parameters;
import com.google.crypto.tink.aead.PredefinedAeadParameters;
import com.google.crypto.tink.daead.PredefinedDeterministicAeadParameters;

public enum DekFormat {
  AES128_GCM(false, PredefinedAeadParameters.AES128_GCM),
  AES256_GCM(false, PredefinedAeadParameters.AES256_GCM),
  AES256_SIV(true, PredefinedDeterministicAeadParameters.AES256_SIV);

  private final boolean isDeterministic;
  private final Parameters parameters;

  DekFormat(boolean isDeterministic, Parameters parameters) {
    this.isDeterministic = isDeterministic;
    this.parameters = parameters;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }

  public Parameters getParameters() {
    return parameters;
  }
}