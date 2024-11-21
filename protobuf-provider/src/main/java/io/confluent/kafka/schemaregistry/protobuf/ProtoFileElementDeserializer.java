/*
 * Copyright 2022 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf;

import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DEFAULT_LOCATION;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.ImmutableList;
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import kotlin.ranges.IntRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProtoFileElementDeserializer extends StdDeserializer<ProtoFileElement> {
  private static final ObjectMapper mapper = JacksonMapper.INSTANCE;

  public ProtoFileElementDeserializer() {
    this(null);
  }

  public ProtoFileElementDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public ProtoFileElement deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    return toProtoFile(node);
  }

  private ProtoFileElement toProtoFile(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<TypeElement> typeElementBuilder = ImmutableList.builder();
    for (JsonNode typeNode : node.get("types")) {
      typeElementBuilder.add(toType(typeNode));
    }

    ImmutableList.Builder<ServiceElement> serviceElementBuilder = ImmutableList.builder();
    for (JsonNode serviceNode : node.get("services")) {
      serviceElementBuilder.add(toService(serviceNode));
    }

    ImmutableList.Builder<ExtendElement> extendElementBuilder = ImmutableList.builder();
    for (JsonNode extendNode : node.get("extendDeclarations")) {
      extendElementBuilder.add(toExtend(extendNode));
    }

    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }


    return new ProtoFileElement(
      toLocation(node.get("location")),
      node.get("packageName").asText(),
      Syntax.valueOf(node.get("syntax").asText()),
      Arrays.asList(mapper.convertValue(node.get("imports"), String[].class)),
      Arrays.asList(mapper.convertValue(node.get("publicImports"), String[].class)),
      typeElementBuilder.build(),
      serviceElementBuilder.build(),
      extendElementBuilder.build(),
      optionElementBuilder.build()
    );
  }

  private TypeElement toType(JsonNode node) throws JsonProcessingException {
    if (node.has("constants")) {
      return toEnum(node);
    } else {
      return toMessage(node);
    }
  }

  private MessageElement toMessage(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<TypeElement> typeElementBuilder = ImmutableList.builder();
    for (JsonNode typeNode : node.get("nestedTypes")) {
      typeElementBuilder.add(toType(typeNode));
    }

    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    ImmutableList.Builder<ReservedElement> reservedElementBuilder = ImmutableList.builder();
    for (JsonNode reservedNode : node.get("reserveds")) {
      reservedElementBuilder.add(toReserved(reservedNode));
    }

    ImmutableList.Builder<FieldElement> fieldElementBuilder = ImmutableList.builder();
    for (JsonNode fieldNode : node.get("fields")) {
      fieldElementBuilder.add(toField(fieldNode));
    }

    ImmutableList.Builder<OneOfElement> oneOfElementBuilder = ImmutableList.builder();
    for (JsonNode oneOfNode : node.get("oneOfs")) {
      oneOfElementBuilder.add(toOneOf(oneOfNode));
    }

    ImmutableList.Builder<ExtensionsElement> extensionsElementBuilder = ImmutableList.builder();
    for (JsonNode extensionNode : node.get("extensions")) {
      extensionsElementBuilder.add(toExtensions(extensionNode));
    }

    ImmutableList.Builder<ExtendElement> extendElementBuilder = ImmutableList.builder();
    for (JsonNode extendNode : node.get("extendDeclarations")) {
      extendElementBuilder.add(toExtend(extendNode));
    }

    // skip group
    return new MessageElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("documentation").asText(),
      typeElementBuilder.build(),
      optionElementBuilder.build(),
      reservedElementBuilder.build(),
      fieldElementBuilder.build(),
      oneOfElementBuilder.build(),
      extensionsElementBuilder.build(),
      Collections.emptyList(),
      extendElementBuilder.build()
    );
  }

  private OneOfElement toOneOf(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<FieldElement> fieldElementBuilder = ImmutableList.builder();
    for (JsonNode fieldNode : node.get("fields")) {
      fieldElementBuilder.add(toField(fieldNode));
    }

    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    /** skip group as <code>ProtobufSchema</code> */
    return new OneOfElement(
      node.get("name").asText(),
      node.get("documentation").asText(),
      fieldElementBuilder.build(),
      Collections.emptyList(),
      optionElementBuilder.build(),
      DEFAULT_LOCATION
    );
  }

  private ExtensionsElement toExtensions(JsonNode node) {
    ImmutableList.Builder<Object> valueBuilder = ImmutableList.builder();
    for (JsonNode value : node.get("values")) {
      if (value.isInt()) {
        valueBuilder.add(value.asInt());
      } else {
        valueBuilder.add(
          new IntRange(value.get("start").asInt(), value.get("endInclusive").asInt()));
      }
    }

    return new ExtensionsElement(
      toLocation(node.get("location")),
      node.get("documentation").asText(),
      valueBuilder.build()
    );
  }

  private EnumElement toEnum(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    ImmutableList.Builder<EnumConstantElement> enumConstantElementBuilder = ImmutableList.builder();
    for (JsonNode enumConstantNode : node.get("constants")) {
      enumConstantElementBuilder.add(toEnumConstant(enumConstantNode));
    }

    ImmutableList.Builder<ReservedElement> reservedElementBuilder = ImmutableList.builder();
    for (JsonNode reservedNode : node.get("reserveds")) {
      reservedElementBuilder.add(toReserved(reservedNode));
    }

    return new EnumElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("documentation").asText(),
      optionElementBuilder.build(),
      enumConstantElementBuilder.build(),
      reservedElementBuilder.build()
    );
  }

  private EnumConstantElement toEnumConstant(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    return new EnumConstantElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("tag").asInt(),
      node.get("documentation").asText(),
      optionElementBuilder.build()
    );
  }

  private FieldElement toField(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    return new FieldElement(
      toLocation(node.get("location")),
      toLabel(node.get("label")),
      node.get("type").asText(),
      node.get("name").asText(),
      nodeToString(node.get("defaultValue")),
      nodeToString(node.get("jsonName")),
      node.get("tag").asInt(),
      node.get("documentation").asText(),
      optionElementBuilder.build()
    );
  }

  private ReservedElement toReserved(JsonNode node) {
    ImmutableList.Builder<Object> valueBuilder = ImmutableList.builder();
    for (JsonNode value : node.get("values")) {
      if (value.isInt()) {
        valueBuilder.add(value.asInt());
      } else if (value.isTextual()) {
        valueBuilder.add(value.asText());
      } else {
        valueBuilder.add(
          new IntRange(value.get("start").asInt(), value.get("endInclusive").asInt()));
      }
    }
    return new ReservedElement(
      toLocation(node.get("location")),
      node.get("documentation").asText(),
      valueBuilder.build()
    );
  }

  private ExtendElement toExtend(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<FieldElement> fieldElementBuilder = ImmutableList.builder();
    for (JsonNode fieldNode : node.get("fields")) {
      fieldElementBuilder.add(toField(fieldNode));
    }

    return new ExtendElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("documentation").asText(),
      fieldElementBuilder.build()
    );
  }

  private ServiceElement toService(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<RpcElement> rpcElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("rpcs")) {
      rpcElementBuilder.add(toRpc(optionNode));
    }

    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    return new ServiceElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("documentation").asText(),
      rpcElementBuilder.build(),
      optionElementBuilder.build()
    );
  }

  private RpcElement toRpc(JsonNode node) throws JsonProcessingException {
    ImmutableList.Builder<OptionElement> optionElementBuilder = ImmutableList.builder();
    for (JsonNode optionNode : node.get("options")) {
      optionElementBuilder.add(toOption(optionNode));
    }

    return new RpcElement(
      toLocation(node.get("location")),
      node.get("name").asText(),
      node.get("documentation").asText(),
      node.get("requestType").asText(),
      node.get("responseType").asText(),
      node.get("requestStreaming").asBoolean(),
      node.get("responseStreaming").asBoolean(),
      optionElementBuilder.build()
    );
  }

  private OptionElement toOption(JsonNode node) throws JsonProcessingException {
    OptionElement.Kind kind = OptionElement.Kind.valueOf(node.get("kind").asText());
    Object value = null;
    JsonNode valueNode = node.get("value");
    switch (kind) {
      case ENUM:
      case STRING:
      case NUMBER:
        value = valueNode.asText();
        break;
      case BOOLEAN:
        value = valueNode.asBoolean();
        break;
      case MAP:
        value = toOptionMap(valueNode);
        break;
      case LIST:
        value = mapper.readValue(valueNode.toString(),
          new TypeReference<ArrayList<String>>() {
          });
        break;
      case OPTION:
        value = toOption(valueNode);
        break;
      default:
        throw new IllegalStateException(
          String.format("Unknown kind '%s' of given option node.",
            node.get("kind").asText()));
    }

    return new OptionElement(
      node.get("name").asText(),
      OptionElement.Kind.valueOf(node.get("kind").asText()),
      value,
      node.get("parenthesized").asBoolean()
    );
  }

  private Map<String, Object> toOptionMap(JsonNode node) {
    Map<String, Object> optionsMap = new LinkedHashMap<>();
    Iterator<String> optionsIter = node.fieldNames();
    while (optionsIter.hasNext()) {
      String option = optionsIter.next();
      JsonNode optionEntryNode = node.get(option);
      optionsMap.put(option, toNestedValue(optionEntryNode));
    }
    return optionsMap;
  }

  private Object toNestedValue(JsonNode node) {
    OptionElement.OptionPrimitive primitiveValue = getOptionPrimitive(node);
    if (primitiveValue != null) {
      return primitiveValue;
    }
    switch (node.getNodeType()) {
      case STRING:
        return node.textValue();
      case NUMBER:
        return node.numberValue();
      case BOOLEAN:
        return node.booleanValue();
      case ARRAY:
        List<Object> nestedList = new ArrayList<>();
        Iterator<JsonNode> arrayIter = node.elements();
        while (arrayIter.hasNext()) {
          nestedList.add(toNestedValue(arrayIter.next()));
        }
        return nestedList;
      case OBJECT:
        return toOptionMap(node);
      default:
        return null;
    }
  }

  private OptionElement.OptionPrimitive getOptionPrimitive(JsonNode node) {
    if (node.has("kind")) {
      String kind = node.get("kind").asText();
      if ("NUMBER".equals(kind)
          || "ENUM".equals(kind)
          || "BOOLEAN".equals(kind)) {
        return new OptionElement.OptionPrimitive(
          OptionElement.Kind.valueOf(kind), node.get("value").asText());
      }
    }
    return null;
  }

  private Location toLocation(JsonNode node) {
    return new Location(node.get("base").asText(),
      node.get("path").asText(),
      node.get("line").asInt(),
      node.get("column").asInt());
  }

  private Field.Label toLabel(JsonNode node) {
    return node.isNull() ? null : Field.Label.valueOf(node.asText());
  }

  private String nodeToString(JsonNode node) {
    return node.isNull() ? null : node.asText();
  }
}

