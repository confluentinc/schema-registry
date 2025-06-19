/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.storage.encoder;

import com.google.crypto.tink.Aead;
import com.google.crypto.tink.KeysetHandle;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.storage.SchemaValue;
import io.kcache.Cache;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

public interface MetadataEncoderServiceInterface {

    void encodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException;

    void decodeMetadata(SchemaValue schema) throws SchemaRegistryStoreException;

    void init();

    void close();

    void transformMetadata(SchemaValue schema, boolean rotationNeeded, boolean isEncode,
                           BiFunction<Aead, String, String> func)
            throws SchemaRegistryStoreException;

    KeysetHandle getOrCreateEncoder(String tenant, Boolean rotationNeeded);
}
