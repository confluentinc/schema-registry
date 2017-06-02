/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.kafka.serializers;

import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.errors.SerializationException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;


public class KafkaAvroValueEncoder extends AbstractKafkaAvroSerializer implements Encoder<Object> {

    public KafkaAvroValueEncoder(SchemaRegistryClient schemaRegistry) {
        this.schemaRegistry = schemaRegistry;
    }

    /**
     * Constructor used by Kafka producer.
     */
    public KafkaAvroValueEncoder(VerifiableProperties props) {
        configure(serializerConfig(props));
    }

    @Override
    public byte[] toBytes(Object object) {
        if (object instanceof GenericContainer) {
            return serializeImpl(getSubjectName(((GenericContainer) object).getSchema().getFullName(), false), object);
        } else {
            throw new SerializationException("Primitive types are not supported yet");
        }
    }
}
