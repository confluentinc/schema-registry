/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.avro;

import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class AvroConverterConfig extends AbstractKafkaAvroSerDeConfig {

  public AvroConverterConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public static class Builder {

    private Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public AvroConverterConfig build() {
      return new AvroConverterConfig(props);
    }
  }
}
