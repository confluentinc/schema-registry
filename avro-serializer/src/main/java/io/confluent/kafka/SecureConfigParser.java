/*
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

package io.confluent.kafka;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import avro.shaded.com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class SecureConfigParser {

  public static void parse(Map<String, Object> configs) {
    parseSchemaRegistryUrl(configs);
  }

  @VisibleForTesting
  protected static void parseSchemaRegistryUrl(Map<String, Object> configs) {
    String originalUrlString =
        ((String) configs.get(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)).trim();
    List<String> urls = Arrays.asList(originalUrlString.split("\\s*,\\s*", -1));
    List<String> userInfoRemovedUrls = new ArrayList<>();
    String userInfo = null;
    for (String urlString : urls) {
      try {
        URL url = new URL(urlString);
        if (url.getUserInfo() != null) {
          urlString = urlString.replaceFirst(url.getUserInfo() + "@", "");
          userInfo = url.getUserInfo();
        }
        userInfoRemovedUrls.add(urlString);
      } catch (MalformedURLException e) {
        // do nothing
      }
    }

    if (userInfo != null) {
      configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, userInfoRemovedUrls);
      configs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_USER_INFO, userInfo);
    }
  }

}
