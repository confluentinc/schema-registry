/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class UriBuilder {

  /**
   * Escapes all characters but the allowed ones in path segments based on
   * https://datatracker.ietf.org/doc/html/rfc3986.
   */
  static class UriPercentEncoder {
    static final String CHARS_UNENCODE;
    private static final BitSet UNENCODE;

    static {
      // 2.2. General delimiters
      String gendelims = "@:";
      // 2.2. Subdelimiters
      String subdelims = "!$&\'()*+,;=";
      // 2.3. Unreserved Characters
      String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
      String unreserved = "-._~";

      CHARS_UNENCODE = alpha + unreserved + gendelims + subdelims;

      BitSet unencode = new BitSet(256);
      for (int i = 0; i < CHARS_UNENCODE.length(); i++) {
        unencode.set(CHARS_UNENCODE.charAt(i));
      }
      UNENCODE = unencode;
    }

    static String encode(String value, Charset charset) {
      StringBuilder sb = new StringBuilder(value.length() * 2);
      for (int i = 0; i < value.length(); i++) {
        char c = value.charAt(i);
        if (UNENCODE.get(c & 0xFF)) {
          sb.append(c);
        } else {
          String hex = Integer.toHexString(c).toUpperCase();
          if (hex.length() == 1) {
            sb.append("%0").append(hex);
          } else {
            sb.append('%').append(hex);
          }
        }
      }
      return sb.toString();
    }

  }

  private final String templatePath;

  private StringBuilder queryParamString = new StringBuilder();
  private final List<String> templateNames;

  public UriBuilder(String templatePath) {
    this.templatePath = Objects.requireNonNull(templatePath);
    this.templateNames = findNamesInTemplate(templatePath);
  }

  public static UriBuilder fromPath(String path) {
    return new UriBuilder(path);
  }

  public URI build(Object... templatePathValues) {

    List<String> templateValues = Arrays.asList(templatePathValues).stream()
        .map(o -> UriPercentEncoder.encode(String.valueOf(o), Charset.defaultCharset()))
        .collect(Collectors.toList());
    if (templateValues.size() != this.templateNames.size()) {
      throw new IllegalArgumentException("Mismatched number of template variable names: expected "
          + this.templateNames.size() + ", got " + templateValues.size());
    }

    String encodedPath = templatePath;
    for (int i = 0; i < templateNames.size(); i++) {
      encodedPath = encodedPath.replace(templateNames.get(i), templateValues.get(i));
    }

    if (queryParamString.length() > 0) {
      if (encodedPath.indexOf('?') < 0) {
        encodedPath += '?';
      }
      encodedPath += queryParamString;
    }

    try {
      return new URI(encodedPath);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public UriBuilder queryParam(String paramName, String paramValue) {
    if (queryParamString.length() > 0) {
      queryParamString.append('&');
    }
    try {
      queryParamString.append(encodeQueryParameter(paramName)).append('=')
          .append(encodeQueryParameter(paramValue));
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
    return this;
  }

  public UriBuilder queryParam(String paramName, Integer paramValue) {
    queryParam(paramName, String.valueOf(paramValue));
    return this;
  }

  public UriBuilder queryParam(String paramName, boolean paramValue) {
    queryParam(paramName, Boolean.toString(paramValue));
    return this;
  }

  @Override
  public String toString() {
    return templatePath;
  }

  static String encodeQueryParameter(String paramValue) throws UnsupportedEncodingException {
    return URLEncoder.encode(paramValue, "UTF-8")
        /*
         * use percent-encoding which is supported everywhere as per RFC-3986, not
         * legacy RFC-1866
         */
        .replace("+", "%20");
  }

  static final List<String> findNamesInTemplate(String path) {
    int counter = 0;
    List<String> templateNames = new ArrayList<>();
    StringBuilder sb = null;
    while (counter < path.length()) {
      char c = path.charAt(counter++);
      if (c == '{' && sb == null) {
        sb = new StringBuilder();
      }
      if (sb != null) {
        sb.append(c);
      }
      if (c == '}' && sb != null) {
        templateNames.add(sb.toString());
        sb = null;
      }
    }
    return Collections.unmodifiableList(templateNames);
  }
}
