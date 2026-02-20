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

package io.confluent.kafka.schemaregistry.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.CacheProvider;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsonFormatVisitors.JsonFormatVisitorWrapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ser.DefaultSerializerProvider;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.impl.UnknownSerializer;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.util.TreeMap;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

/**
 * A utility class for Jackson.
 */
public class Jackson {
  private Jackson() {
    /* singleton */
  }

  /**
   * Creates a new {@link ObjectMapper}.
   */
  public static ObjectMapper newObjectMapper() {
    return newObjectMapper(false);
  }

  /**
   * Creates a new {@link ObjectMapper}.
   *
   * @param sorted whether to sort object properties
   */
  public static ObjectMapper newObjectMapper(boolean sorted) {
    final ObjectMapper mapper = JsonMapper.builder()
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .build();

    return configure(mapper, sorted);
  }

  /**
   * Creates a new {@link ObjectMapper} with a custom
   * {@link com.fasterxml.jackson.core.JsonFactory}.
   *
   * @param jsonFactory instance of {@link com.fasterxml.jackson.core.JsonFactory} to use
   *     for the created {@link com.fasterxml.jackson.databind.ObjectMapper} instance.
   */
  public static ObjectMapper newObjectMapper(JsonFactory jsonFactory) {
    final ObjectMapper mapper = JsonMapper.builder(jsonFactory)
        .enable(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS)
        .build();

    return configure(mapper, false);
  }

  private static ObjectMapper configure(ObjectMapper mapper, boolean sorted) {
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new JodaModule());
    mapper.registerModule(new ParameterNamesModule());
    mapper.registerModule(new Jdk8Module());
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new JsonOrgModule());
    mapper.registerModule(new JsonSkemaModule());
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setNodeFactory(sorted
        ? new SortingNodeFactory(true)
        : JsonNodeFactory.withExactBigDecimals(true));
    mapper.setSerializerProvider(new DefaultSerializerProviderImpl());

    return mapper;
  }

  static class SortingNodeFactory extends JsonNodeFactory {
    public SortingNodeFactory(boolean bigDecimalExact) {
      super(bigDecimalExact);
    }

    @Override
    public ObjectNode objectNode() {
      return new ObjectNode(this, new TreeMap<>());
    }
  }

  static class DefaultSerializerProviderImpl extends DefaultSerializerProvider {
    private static final long serialVersionUID = 1L;
    protected static final JsonSerializer<Object> DEFAULT_UNKNOWN_SERIALIZER
        = new UnknownSerializerImpl();

    public DefaultSerializerProviderImpl() {
      super();
      _unknownTypeSerializer = DEFAULT_UNKNOWN_SERIALIZER;
    }

    public DefaultSerializerProviderImpl(DefaultSerializerProviderImpl src) {
      super(src);
      _unknownTypeSerializer = DEFAULT_UNKNOWN_SERIALIZER;
    }

    public DefaultSerializerProviderImpl(DefaultSerializerProviderImpl src, CacheProvider cp) {
      super(src, cp);
      _unknownTypeSerializer = DEFAULT_UNKNOWN_SERIALIZER;
    }

    protected DefaultSerializerProviderImpl(
        SerializerProvider src, SerializationConfig config, SerializerFactory f) {
      super(src, config, f);
      _unknownTypeSerializer = DEFAULT_UNKNOWN_SERIALIZER;
    }

    public DefaultSerializerProvider copy() {
      return this.getClass() != DefaultSerializerProviderImpl.class
          ? super.copy()
          : new DefaultSerializerProviderImpl(this);
    }

    public DefaultSerializerProviderImpl createInstance(
        SerializationConfig config, SerializerFactory jsf) {
      return new DefaultSerializerProviderImpl(this, config, jsf);
    }

    public DefaultSerializerProviderImpl withCaches(CacheProvider cp) {
      return new DefaultSerializerProviderImpl(this, cp);
    }
  }

  static class UnknownSerializerImpl extends UnknownSerializer {
    public UnknownSerializerImpl() {
      super();
    }

    public UnknownSerializerImpl(Class<?> cls) {
      super(cls);
    }

    /*
     * Starting with Jackson 2.13, UnknownSerializer calls visitor.expectObjectFormat
     * instead of visitor.expectAnyFormat.  Here we maintain the pre-2.13 functionality
     * until mbknor-jackson-jsonSchema ever changes it's behavior w.r.t. Jackson 2.13
     * See https://github.com/FasterXML/jackson-dataformats-binary/issues/281
     */
    public void acceptJsonFormatVisitor(JsonFormatVisitorWrapper visitor, JavaType typeHint)
        throws JsonMappingException {
      visitor.expectAnyFormat(typeHint);
    }
  }
}
