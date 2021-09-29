/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.json;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.json.DecimalFormat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class JsonSchemaDataConfig extends AbstractConfig {

  public static final String OBJECT_ADDITIONAL_PROPERTIES_CONFIG = "object.additional.properties";
  public static final boolean OBJECT_ADDITIONAL_PROPERTIES_DEFAULT = true;
  public static final String OBJECT_ADDITIONAL_PROPERTIES_DOC =
      "Whether to allow additional properties for object schemas.";

  public static final String USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG = "use.optional.for.nonrequired";
  public static final boolean USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT = false;
  public static final String USE_OPTIONAL_FOR_NON_REQUIRED_DOC =
      "Whether to set non-required properties to be optional.";

  public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
  public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
  public static final String SCHEMAS_CACHE_SIZE_DOC = "Size of the converted schemas cache";

  public static final String DECIMAL_FORMAT_CONFIG = "decimal.format";
  public static final String DECIMAL_FORMAT_DEFAULT = DecimalFormat.BASE64.name();
  private static final String DECIMAL_FORMAT_DOC =
      "Controls which format this converter will serialize decimals in."
      + " This value is case insensitive and can be either 'BASE64' (default) or 'NUMERIC'";

  public static ConfigDef baseConfigDef() {
    return new ConfigDef().define(
        OBJECT_ADDITIONAL_PROPERTIES_CONFIG,
        ConfigDef.Type.BOOLEAN,
        OBJECT_ADDITIONAL_PROPERTIES_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        OBJECT_ADDITIONAL_PROPERTIES_DOC
    ).define(
        USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG,
        ConfigDef.Type.BOOLEAN,
        USE_OPTIONAL_FOR_NON_REQUIRED_DEFAULT,
        ConfigDef.Importance.MEDIUM,
        USE_OPTIONAL_FOR_NON_REQUIRED_DOC
    ).define(
        SCHEMAS_CACHE_SIZE_CONFIG,
        ConfigDef.Type.INT,
        SCHEMAS_CACHE_SIZE_DEFAULT,
        ConfigDef.Importance.LOW,
        SCHEMAS_CACHE_SIZE_DOC
    ).define(
        DECIMAL_FORMAT_CONFIG,
        ConfigDef.Type.STRING,
        DECIMAL_FORMAT_DEFAULT,
        CaseInsensitiveValidString.in(
            DecimalFormat.BASE64.name(),
            DecimalFormat.NUMERIC.name()),
        ConfigDef.Importance.LOW,
        DECIMAL_FORMAT_DOC);
  }

  public JsonSchemaDataConfig(Map<?, ?> props) {
    super(baseConfigDef(), props);
  }

  public boolean allowAdditionalProperties() {
    return getBoolean(OBJECT_ADDITIONAL_PROPERTIES_CONFIG);
  }

  public boolean useOptionalForNonRequiredProperties() {
    return getBoolean(USE_OPTIONAL_FOR_NON_REQUIRED_CONFIG);
  }

  public int schemaCacheSize() {
    return Math.max(1, this.getInt(SCHEMAS_CACHE_SIZE_CONFIG));
  }

  /**
   * Get the serialization format for decimal types.
   *
   * @return the decimal serialization format
   */
  public DecimalFormat decimalFormat() {
    return DecimalFormat.valueOf(getString(DECIMAL_FORMAT_CONFIG).toUpperCase(Locale.ROOT));
  }

  public static class Builder {

    private final Map<String, Object> props = new HashMap<>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public JsonSchemaDataConfig build() {
      return new JsonSchemaDataConfig(props);
    }
  }

  public static class CaseInsensitiveValidString implements ConfigDef.Validator {

    final Set<String> validStrings;

    private CaseInsensitiveValidString(List<String> validStrings) {
      this.validStrings = validStrings.stream()
          .map(s -> s.toUpperCase(Locale.ROOT))
          .collect(Collectors.toSet());
    }

    public static CaseInsensitiveValidString in(String... validStrings) {
      return new CaseInsensitiveValidString(Arrays.asList(validStrings));
    }

    @Override
    public void ensureValid(String name, Object o) {
      String s = (String) o;
      if (s == null || !validStrings.contains(s.toUpperCase(Locale.ROOT))) {
        throw new ConfigException(name, o, "String must be one of (case insensitive): "
            + Utils.join(validStrings, ", "));
      }
    }

    public String toString() {
      return "(case insensitive) [" + Utils.join(validStrings, ", ") + "]";
    }
  }
}
