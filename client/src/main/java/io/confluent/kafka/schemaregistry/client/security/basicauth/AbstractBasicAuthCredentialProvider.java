/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.basicauth;

import io.confluent.common.config.ConfigException;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class AbstractBasicAuthCredentialProvider implements BasicAuthCredentialProvider {

  @Override
  public void configure(Map<String, ?> configs) throws ConfigException {
    // do nothing as default
  }

  static String decodeUserInfo(String userInfo) {
    if (userInfo == null) {
      return null;
    }
    try {
      return URLDecoder.decode(userInfo, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("Java Runtime does not support UTF-8 for URL decoding.");
    }
  }
}
