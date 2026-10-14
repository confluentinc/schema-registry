/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.connect.backup.bytearray;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Map;

/**
 * Config for {@link BackupAwareByteArrayConverter}. Inherits all standard Confluent SR knobs
 * from {@link AbstractKafkaSchemaSerDeConfig}, including {@code schema.registry.url},
 * {@code key.subject.name.strategy} / {@code value.subject.name.strategy},
 * {@code key.schema.id.serializer} / {@code value.schema.id.serializer}, etc.
 *
 * <p>Backup mode is toggled via {@code schema.backup.enabled} (see
 * {@code io.confluent.connect.schema.backup.api.SchemaBackupConfig}).
 */
public class BackupAwareByteArrayConverterConfig extends AbstractKafkaSchemaSerDeConfig {

  public BackupAwareByteArrayConverterConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }
}
