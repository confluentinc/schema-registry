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
 *
 */

package io.confluent.kafka.schemaregistry.protobuf;

import static com.squareup.wire.schema.internal.UtilKt.MAX_TAG_VALUE;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.CONFLUENT_PREFIX;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.DEFAULT_LOCATION;
import static io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.transform;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Ascii;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import com.squareup.wire.Syntax;
import com.squareup.wire.schema.Field.Label;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.GroupElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.OptionElement.Kind;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.RpcElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context.TypeElementInfo;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import kotlin.Pair;
import kotlin.ranges.IntRange;
import org.apache.commons.lang3.math.NumberUtils;

public class ProtobufSchemaUtils {

  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;

  private static final ObjectMapper mapperWithProtoFileDeserializer = new ObjectMapper();
  private static final SimpleModule module = new SimpleModule();

  static {
    module.addDeserializer(ProtoFileElement.class, new ProtoFileElementDeserializer());
    mapperWithProtoFileDeserializer.registerModule(module);
  }

  public static ProtobufSchema copyOf(ProtobufSchema schema) {
    return schema.copy();
  }

  public static ProtobufSchema getSchema(Message message) {
    return message != null ? new ProtobufSchema(message.getDescriptorForType()) : null;
  }

  public static Object toObject(JsonNode value, ProtobufSchema schema) throws IOException {
    StringWriter out = new StringWriter();
    jsonMapper.writeValue(out, value);
    return toObject(out.toString(), schema);
  }

  public static Object toObject(String value, ProtobufSchema schema)
      throws InvalidProtocolBufferException {
    DynamicMessage.Builder message = schema.newMessageBuilder();
    JsonFormat.parser().merge(value, message);
    return message.build();
  }

  public static byte[] toJson(Message message) throws IOException {
    if (message == null) {
      return null;
    }
    String jsonString = JsonFormat.printer()
        .includingDefaultValueFields()
        .omittingInsignificantWhitespace()
        .print(message);
    return jsonString.getBytes(StandardCharsets.UTF_8);
  }

  public static ProtoFileElement jsonToFile(JsonNode node) throws JsonProcessingException {
    return mapperWithProtoFileDeserializer.convertValue(node, ProtoFileElement.class);
  }

  public static JsonNode findMatchingNodeHelper(JsonNode node, String arrayField,
                                                String targetFieldName, String targetFieldValue) {
    Iterator<JsonNode> iter = node.get(arrayField).elements();
    while (iter.hasNext()) {
      JsonNode currNode = iter.next();
      if (targetFieldValue.equals(currNode.get(targetFieldName).asText())) {
        return currNode;
      }
    }
    return null;
  }

  public static JsonNode findMatchingNode(JsonNode node, String[] identifiers,
                                          boolean isFieldPath) {
    JsonNode nodePtr = null;
    // the idx of last message identifier (exclusive)
    int end = isFieldPath ? identifiers.length - 1 : identifiers.length;
    int idx = 0;
    for (; idx < end; idx++) {
      JsonNode found;
      String targetName = identifiers[idx];
      if (idx == 0) {
        found = findMatchingNodeHelper(node, "types", "name", targetName);
      } else {
        found = findMatchingNodeHelper(nodePtr, "nestedTypes", "name", targetName);
      }

      if (found != null) {
        nodePtr = found;
      } else if (idx == end - 1 && isFieldPath) {
        // if the last message cannot be found, it could be the last identifier is the oneof
        // field's name, return the previous messageElement in this case
        break;
      } else {
        throw new IllegalArgumentException(
            String.format("No matching Message with name '%s' found in the schema", targetName));
      }
    }

    if (!isFieldPath) {
      return nodePtr;
    }

    // need to find the fieldNode
    JsonNode fieldNode = null;
    String fieldName = identifiers[identifiers.length - 1];
    if (nodePtr == null) {
      throw new IllegalArgumentException(
          String.format("No matching Field with name '%s' found in the schema", fieldName));
    }
    if (idx == end) {
      fieldNode = findMatchingNodeHelper(nodePtr, "fields", "name", fieldName);
      if (fieldNode == null) {
        // find in oneOf fields
        Iterator<JsonNode> oneOfs = nodePtr.get("oneOfs").elements();
        while (oneOfs.hasNext()) {
          JsonNode currNode = oneOfs.next();
          fieldNode = findMatchingNodeHelper(currNode, "fields", "name", fieldName);
          if (fieldNode != null) {
            return fieldNode;
          }
        }
      }
    } else {
      // path may contain oneOf fieldName
      JsonNode oneOfNode = findMatchingNodeHelper(nodePtr, "oneOfs", "name", identifiers[idx]);
      if (oneOfNode != null) {
        fieldNode = findMatchingNodeHelper(oneOfNode, "fields", "name", fieldName);
      }
    }
    if (fieldNode != null) {
      return fieldNode;
    } else {
      throw new IllegalArgumentException(
          String.format("No matching Field with name '%s' found in the schema", fieldName));
    }
  }

  public static MessageElement findMatchingMessage(List<TypeElement> typeElementList, String name) {
    for (TypeElement typeElement : typeElementList) {
      if (typeElement instanceof MessageElement && name.equals(typeElement.getName())) {
        return (MessageElement) typeElement;
      }
    }
    return null;
  }

  public static Object findMatchingElement(ProtoFileElement original,
                                           String[] identifiers,
                                           boolean isFieldPath) {
    MessageElement messageElement = null;
    int i = 0;
    // the idx of last message identifier (exclusive)
    int end = isFieldPath ? identifiers.length - 1 : identifiers.length;

    for (; i < end; i++) {
      MessageElement found;
      String targetName = identifiers[i];
      if (i == 0) {
        found = findMatchingMessage(original.getTypes(), targetName);
      } else {
        found = findMatchingMessage(messageElement.getNestedTypes(), targetName);
      }
      if (found != null) {
        messageElement = found;
      } else if (i == end - 1 && isFieldPath) {
        // if the last message cannot be found, it could be the last identifier is the oneof
        // field's name, return the previous messageElement in this case
        break;
      } else {
        throw new IllegalArgumentException(
            String.format("No matching Message with name '%s' found in the schema", targetName));
      }
    }

    if (!isFieldPath) {
      return messageElement;
    }

    // need to find FieldElement
    String fieldName = identifiers[identifiers.length - 1];
    if (messageElement == null) {
      throw new IllegalArgumentException(
          String.format("No matching Field with name '%s' found in the schema", fieldName));
    }
    if (i == end) {
      for (FieldElement fieldElement : messageElement.getFields()) {
        if (fieldName.equals(fieldElement.getName())) {
          return fieldElement;
        }
      }
      // search oneOf fields
      for (OneOfElement oneOfElement : messageElement.getOneOfs()) {
        for (FieldElement fieldElement : oneOfElement.getFields()) {
          if (fieldName.equals(fieldElement.getName())) {
            return fieldElement;
          }
        }
      }
    } else {
      // path may contain oneOf fieldName
      String oneOfFieldName = identifiers[i];
      OneOfElement foundOneOfElement = null;
      for (OneOfElement oneOfElement : messageElement.getOneOfs()) {
        if (oneOfFieldName.equals(oneOfElement.getName())) {
          foundOneOfElement = oneOfElement;
        }
      }
      if (foundOneOfElement != null) {
        for (FieldElement fieldElement : foundOneOfElement.getFields()) {
          if (fieldName.equals(fieldElement.getName())) {
            return fieldElement;
          }
        }
      }
    }
    throw new IllegalArgumentException(
        String.format("No matching Field with name '%s' found in the schema", fieldName));
  }

  protected static String toNormalizedString(ProtobufSchema schema) {
    FormatContext ctx = new FormatContext(false, true);
    return toFormattedString(ctx, schema);
  }

  protected static String toFormattedString(FormatContext ctx, ProtobufSchema schema) {
    if (ctx.normalize()) {
      ctx.collectTypeInfo(schema, true);
    }
    return toString(ctx, schema.rawSchema());
  }

  protected static String toString(ProtoFileElement protoFile) {
    FormatContext ctx = new FormatContext(false, false);
    return toString(ctx, protoFile);
  }

  private static String toString(FormatContext ctx, ProtoFileElement protoFile) {
    StringBuilder sb = new StringBuilder();
    if (protoFile.getSyntax() != null) {
      if (!ctx.normalize() || protoFile.getSyntax() == Syntax.PROTO_3) {
        sb.append("syntax = \"");
        sb.append(protoFile.getSyntax());
        sb.append("\";\n");
      }
    }
    if (protoFile.getPackageName() != null) {
      sb.append("package ");
      sb.append(protoFile.getPackageName());
      sb.append(";\n");
    }
    if (!protoFile.getImports().isEmpty() || !protoFile.getPublicImports().isEmpty()) {
      sb.append('\n');
      List<String> imports = protoFile.getImports();
      if (ctx.normalize()) {
        imports = imports.stream().sorted().distinct().collect(Collectors.toList());
      }
      for (String file : imports) {
        sb.append("import \"");
        sb.append(file);
        sb.append("\";\n");
      }
      List<String> publicImports = protoFile.getPublicImports();
      if (ctx.normalize()) {
        publicImports = publicImports.stream().sorted().distinct().collect(Collectors.toList());
      }
      for (String file : publicImports) {
        sb.append("import public \"");
        sb.append(file);
        sb.append("\";\n");
      }
    }
    List<OptionElement> options = ctx.filterOptions(protoFile.getOptions());
    if (!options.isEmpty()) {
      sb.append('\n');
      for (OptionElement option : options) {
        sb.append(toOptionString(ctx, option));
      }
    }
    List<TypeElement> types = filterTypes(ctx, protoFile.getTypes());
    if (!types.isEmpty()) {
      sb.append('\n');
      // Order of message types is significant since the client is using
      // the non-normalized schema to serialize message indexes
      for (TypeElement typeElement : types) {
        if (typeElement instanceof MessageElement) {
          try (Context.NamedScope nameScope = ctx.enterName(typeElement.getName())) {
            sb.append(toString(ctx, (MessageElement) typeElement));
          }
        }
      }
      for (TypeElement typeElement : types) {
        if (typeElement instanceof EnumElement) {
          try (Context.NamedScope nameScope = ctx.enterName(typeElement.getName())) {
            sb.append(toString(ctx, (EnumElement) typeElement));
          }
        }
      }
    }
    if (!ctx.ignoreExtensions() && !protoFile.getExtendDeclarations().isEmpty()) {
      sb.append('\n');
      List<ExtendElement> extendElems = protoFile.getExtendDeclarations();
      if (ctx.normalize()) {
        extendElems = extendElems.stream()
            .flatMap(e -> e.getFields().stream().map(f -> new Pair<>(resolve(ctx, e.getName()), f)))
            .collect(Collectors.groupingBy(
                Pair::getFirst,
                LinkedHashMap::new,  // deterministic order
                Collectors.mapping(Pair::getSecond, Collectors.toList()))
            )
            .entrySet()
            .stream()
            .map(e -> new ExtendElement(DEFAULT_LOCATION, e.getKey(), "", e.getValue()))
            .collect(Collectors.toList());
      }
      for (ExtendElement extendElem : extendElems) {
        sb.append(toString(ctx, extendElem));
      }
    }
    if (!protoFile.getServices().isEmpty()) {
      sb.append('\n');
      // Don't sort service elements to be consistent with the fact that
      // we don't sort message/enum elements
      for (ServiceElement service : protoFile.getServices()) {
        sb.append(toString(ctx, service));
      }
    }
    return sb.toString();
  }

  private static String toString(FormatContext ctx, ServiceElement service) {
    StringBuilder sb = new StringBuilder();
    sb.append("service ");
    sb.append(service.getName());
    sb.append(" {");
    List<OptionElement> options = ctx.filterOptions(service.getOptions());
    if (!options.isEmpty()) {
      sb.append('\n');
      for (OptionElement option : options) {
        appendIndented(sb, toOptionString(ctx, option));
      }
    }
    if (!service.getRpcs().isEmpty()) {
      sb.append('\n');
      // Don't sort rpc elements to be consistent with the fact that
      // we don't sort message/enum elements
      for (RpcElement rpc : service.getRpcs()) {
        appendIndented(sb, toString(ctx, rpc));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, RpcElement rpc) {
    StringBuilder sb = new StringBuilder();
    sb.append("rpc ");
    sb.append(rpc.getName());
    sb.append(" (");

    if (rpc.getRequestStreaming()) {
      sb.append("stream ");
    }
    String requestType = rpc.getRequestType();
    if (ctx.normalize()) {
      requestType = resolve(ctx, requestType);
    }
    sb.append(requestType);
    sb.append(") returns (");

    if (rpc.getResponseStreaming()) {
      sb.append("stream ");
    }
    String responseType = rpc.getResponseType();
    if (ctx.normalize()) {
      responseType = resolve(ctx, responseType);
    }
    sb.append(responseType);
    sb.append(")");

    List<OptionElement> options = ctx.filterOptions(rpc.getOptions());
    if (!options.isEmpty()) {
      sb.append(" {\n");
      for (OptionElement option : options) {
        appendIndented(sb, toOptionString(ctx, option));
      }
      sb.append('}');
    }

    sb.append(";\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, EnumElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("enum ");
    sb.append(type.getName());
    sb.append(" {");

    if (!type.getReserveds().isEmpty()) {
      sb.append('\n');
      List<ReservedElement> reserveds = type.getReserveds();
      if (ctx.normalize()) {
        reserveds = reserveds.stream()
            .flatMap(r -> r.getValues().stream()
                .map(o -> new ReservedElement(
                    r.getLocation(),
                    r.getDocumentation(),
                    Collections.singletonList(o))
                )
            )
            .collect(Collectors.toList());
        Comparator<Object> cmp = Comparator.comparing(r -> {
          Object o = ((ReservedElement)r).getValues().get(0);
          if (o instanceof IntRange) {
            return ((IntRange) o).getStart();
          } else if (o instanceof Integer) {
            return (Integer) o;
          } else {
            return Integer.MAX_VALUE;
          }
        }).thenComparing(r -> ((ReservedElement) r).getValues().get(0).toString());
        reserveds.sort(cmp);
      }
      for (ReservedElement reserved : reserveds) {
        appendIndented(sb, toString(ctx, reserved));
      }
    }

    if (type.getReserveds().isEmpty()
        && (!type.getOptions().isEmpty() || !type.getConstants().isEmpty())) {
      sb.append('\n');
    }

    List<OptionElement> options = ctx.filterOptions(type.getOptions());
    if (!options.isEmpty()) {
      for (OptionElement option : options) {
        appendIndented(sb, toOptionString(ctx, option));
      }
    }
    if (!type.getConstants().isEmpty()) {
      List<EnumConstantElement> constants = type.getConstants();
      if (ctx.normalize()) {
        constants = new ArrayList<>(constants);
        constants.sort(Comparator
            .comparing(EnumConstantElement::getTag)
            .thenComparing(EnumConstantElement::getName));
      }
      for (EnumConstantElement constant : constants) {
        appendIndented(sb, toString(ctx, constant));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, EnumConstantElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append(type.getName());
    sb.append(" = ");
    sb.append(type.getTag());

    List<OptionElement> options = ctx.filterOptions(type.getOptions());
    if (!options.isEmpty()) {
      sb.append(" ");
      appendOptions(ctx, sb, options);
    }
    sb.append(";\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, MessageElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("message ");
    sb.append(type.getName());
    sb.append(" {");

    if (!type.getReserveds().isEmpty()) {
      sb.append('\n');
      List<ReservedElement> reserveds = type.getReserveds();
      if (ctx.normalize()) {
        reserveds = reserveds.stream()
            .flatMap(r -> r.getValues().stream()
                .map(o -> new ReservedElement(
                    r.getLocation(),
                    r.getDocumentation(),
                    Collections.singletonList(o))
                )
            )
            .collect(Collectors.toList());
        Comparator<Object> cmp = Comparator.comparing(r -> {
          Object o = ((ReservedElement)r).getValues().get(0);
          if (o instanceof IntRange) {
            return ((IntRange) o).getStart();
          } else if (o instanceof Integer) {
            return (Integer) o;
          } else {
            return Integer.MAX_VALUE;
          }
        }).thenComparing(r -> ((ReservedElement) r).getValues().get(0).toString());
        reserveds.sort(cmp);
      }
      for (ReservedElement reserved : reserveds) {
        appendIndented(sb, toString(ctx, reserved));
      }
    }
    List<OptionElement> options = ctx.filterOptions(type.getOptions());
    if (!options.isEmpty()) {
      sb.append('\n');
      for (OptionElement option : options) {
        appendIndented(sb, toOptionString(ctx, option));
      }
    }
    if (!type.getFields().isEmpty()) {
      sb.append('\n');
      List<FieldElement> fields = type.getFields();
      if (ctx.normalize()) {
        fields = new ArrayList<>(fields);
        fields.sort(Comparator.comparing(FieldElement::getTag));
      }
      for (FieldElement field : fields) {
        appendIndented(sb, toString(ctx, field));
      }
    }
    if (!type.getOneOfs().isEmpty()) {
      sb.append('\n');
      List<OneOfElement> oneOfs = type.getOneOfs();
      if (ctx.normalize()) {
        oneOfs = oneOfs.stream()
            .filter(o -> !o.getFields().isEmpty())
            .map(o -> {
              List<FieldElement> fields = new ArrayList<>(o.getFields());
              fields.sort(Comparator.comparing(FieldElement::getTag));
              return new OneOfElement(o.getName(), o.getDocumentation(),
                  fields, o.getGroups(), o.getOptions(), DEFAULT_LOCATION);
            })
            .collect(Collectors.toList());
        oneOfs.sort(Comparator.comparing(o -> o.getFields().get(0).getTag()));
      }
      for (OneOfElement oneOf : oneOfs) {
        appendIndented(sb, toString(ctx, oneOf));
      }
    }
    if (!type.getGroups().isEmpty()) {
      sb.append('\n');
      List<GroupElement> groups = type.getGroups();
      if (ctx.normalize()) {
        groups = new ArrayList<>(groups);
        groups.sort(Comparator.comparing(GroupElement::getTag));
      }
      for (GroupElement group : groups) {
        appendIndented(sb, toString(ctx, group));
      }
    }
    if (!ctx.ignoreExtensions() && !type.getExtensions().isEmpty()) {
      sb.append('\n');
      List<ExtensionsElement> extensions = type.getExtensions();
      if (ctx.normalize()) {
        extensions = extensions.stream()
            .flatMap(r -> r.getValues().stream()
                .map(o -> new ExtensionsElement(
                    r.getLocation(),
                    r.getDocumentation(),
                    Collections.singletonList(o))
                )
            )
            .collect(Collectors.toList());
        Comparator<Object> cmp = Comparator.comparing(r -> {
          Object o = ((ExtensionsElement)r).getValues().get(0);
          if (o instanceof IntRange) {
            return ((IntRange) o).getStart();
          } else if (o instanceof Integer) {
            return (Integer) o;
          } else {
            return Integer.MAX_VALUE;
          }
        });
        extensions.sort(cmp);
      }
      for (ExtensionsElement extension : extensions) {
        appendIndented(sb, toString(ctx, extension));
      }
    }
    if (!ctx.ignoreExtensions() && !type.getExtendDeclarations().isEmpty()) {
      sb.append('\n');
      List<ExtendElement> extendElems = type.getExtendDeclarations();
      if (ctx.normalize()) {
        extendElems = extendElems.stream()
            .flatMap(e -> e.getFields().stream().map(f -> new Pair<>(resolve(ctx, e.getName()), f)))
            .collect(Collectors.groupingBy(
                Pair::getFirst,
                LinkedHashMap::new,  // deterministic order
                Collectors.mapping(Pair::getSecond, Collectors.toList()))
            )
            .entrySet()
            .stream()
            .map(e -> new ExtendElement(DEFAULT_LOCATION, e.getKey(), "", e.getValue()))
            .collect(Collectors.toList());
      }
      for (ExtendElement extendElem : extendElems) {
        appendIndented(sb, toString(ctx, extendElem));
      }
    }
    List<TypeElement> types = filterTypes(ctx, type.getNestedTypes());
    if (!types.isEmpty()) {
      sb.append('\n');
      // Order of message types is significant since the client is using
      // the non-normalized schema to serialize message indexes
      for (TypeElement typeElement : types) {
        if (typeElement instanceof MessageElement) {
          try (Context.NamedScope nameScope = ctx.enterName(typeElement.getName())) {
            appendIndented(sb, toString(ctx, (MessageElement) typeElement));
          }
        }
      }
      for (TypeElement typeElement : types) {
        if (typeElement instanceof EnumElement) {
          try (Context.NamedScope nameScope = ctx.enterName(typeElement.getName())) {
            appendIndented(sb, toString(ctx, (EnumElement) typeElement));
          }
        }
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, ReservedElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("reserved ");

    boolean first = true;
    for (Object value : type.getValues()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      if (value instanceof String) {
        sb.append("\"");
        sb.append(value);
        sb.append("\"");
      } else if (value instanceof Integer) {
        sb.append(value);
      } else if (value instanceof IntRange) {
        IntRange range = (IntRange) value;
        if (ctx.normalize() && range.getStart().equals(range.getEndInclusive())) {
          sb.append(range.getStart());
        } else {
          sb.append(range.getStart());
          sb.append(" to ");
          int last = range.getEndInclusive();
          if (last < MAX_TAG_VALUE) {
            sb.append(last);
          } else {
            sb.append("max");
          }
        }
      } else {
        throw new IllegalArgumentException();
      }
    }
    sb.append(";\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, ExtensionsElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("extensions ");

    boolean first = true;
    for (Object value : type.getValues()) {
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      if (value instanceof Integer) {
        sb.append(value);
      } else if (value instanceof IntRange) {
        IntRange range = (IntRange) value;
        if (ctx.normalize() && range.getStart().equals(range.getEndInclusive())) {
          sb.append(range.getStart());
        } else {
          sb.append(range.getStart());
          sb.append(" to ");
          int last = range.getEndInclusive();
          if (last < MAX_TAG_VALUE) {
            sb.append(last);
          } else {
            sb.append("max");
          }
        }
      } else {
        throw new IllegalArgumentException();
      }
    }
    sb.append(";\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, OneOfElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("oneof ");
    sb.append(type.getName());
    sb.append(" {");

    List<OptionElement> options = ctx.filterOptions(type.getOptions());
    if (!options.isEmpty()) {
      sb.append('\n');
      for (OptionElement option : options) {
        appendIndented(sb, toOptionString(ctx, option));
      }
    }
    if (!type.getFields().isEmpty()) {
      sb.append('\n');
      // Fields have already been sorted while sorting oneOfs in the calling method
      List<FieldElement> fields = type.getFields();
      for (FieldElement field : fields) {
        appendIndented(sb, toString(ctx, field));
      }
    }
    if (!type.getGroups().isEmpty()) {
      sb.append('\n');
      List<GroupElement> groups = type.getGroups();
      if (ctx.normalize()) {
        groups = new ArrayList<>(groups);
        groups.sort(Comparator.comparing(GroupElement::getTag));
      }
      for (GroupElement group : groups) {
        appendIndented(sb, toString(ctx, group));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, GroupElement group) {
    StringBuilder sb = new StringBuilder();
    Label label = group.getLabel();
    if (label != null) {
      sb.append(label.name().toLowerCase(Locale.US));
      sb.append(" ");
    }
    sb.append("group ");
    sb.append(group.getName());
    sb.append(" = ");
    sb.append(group.getTag());
    sb.append(" {");
    if (!group.getFields().isEmpty()) {
      sb.append('\n');
      List<FieldElement> fields = group.getFields();
      if (ctx.normalize()) {
        fields = new ArrayList<>(fields);
        fields.sort(Comparator.comparing(FieldElement::getTag));
      }
      for (FieldElement field : fields) {
        appendIndented(sb, toString(ctx, field));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, ExtendElement extendElem) {
    StringBuilder sb = new StringBuilder();
    sb.append("extend ");
    // Names have been resolved when grouping by name
    String extendName = extendElem.getName();
    sb.append(extendName);
    sb.append(" {");
    if (!extendElem.getFields().isEmpty()) {
      sb.append('\n');
      List<FieldElement> fields = extendElem.getFields();
      if (ctx.normalize()) {
        fields = new ArrayList<>(fields);
        fields.sort(Comparator.comparing(FieldElement::getTag));
      }
      for (FieldElement field : fields) {
        appendIndented(sb, toString(ctx, field));
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(FormatContext ctx, FieldElement field) {
    StringBuilder sb = new StringBuilder();
    Label label = field.getLabel();
    String fieldType = field.getType();
    ProtoType fieldProtoType = ProtoType.get(fieldType);
    if (ctx.normalize()) {
      if (!fieldProtoType.isScalar() && !fieldProtoType.isMap()) {
        // See if the fieldType resolves to a message representing a map
        fieldType = resolve(ctx, fieldType);
        TypeElementInfo typeInfo = ctx.getTypeForFullName(fieldType, true);
        if (typeInfo != null && typeInfo.isMap()) {
          fieldProtoType = typeInfo.getMapType();
        } else {
          fieldProtoType = ProtoType.get(fieldType);
        }
      }
      ProtoType mapValueType = fieldProtoType.getValueType();
      if (fieldProtoType.isMap() && mapValueType != null) {
        // Ensure the value of the map is fully resolved
        String valueType = ctx.resolve(mapValueType.toString(), true);
        if (valueType != null) {
          fieldProtoType = ProtoType.get(
              // Note we add a leading dot to valueType
              "map<" + fieldProtoType.getKeyType() + ", ." + valueType + ">"
          );
        }
        label = null;  // don't emit label for map
      }
      fieldType = fieldProtoType.toString();
    }
    if (label != null) {
      sb.append(label.name().toLowerCase(Locale.US));
      sb.append(" ");
    }
    sb.append(fieldType);
    sb.append(" ");
    sb.append(field.getName());
    sb.append(" = ");
    sb.append(field.getTag());

    List<OptionElement> optionsWithSpecialValues = new ArrayList<>(field.getOptions());
    String defaultValue = field.getDefaultValue();
    if (defaultValue != null) {
      optionsWithSpecialValues.add(
          OptionElement.Companion.create("default", toKind(fieldProtoType), defaultValue));
    }
    String jsonName = field.getJsonName();
    if (jsonName != null) {
      optionsWithSpecialValues.add(
          OptionElement.Companion.create("json_name", Kind.STRING, jsonName));
    }
    optionsWithSpecialValues = ctx.filterOptions(optionsWithSpecialValues);
    if (!optionsWithSpecialValues.isEmpty()) {
      sb.append(" ");
      appendOptions(ctx, sb, optionsWithSpecialValues);
    }

    sb.append(";\n");
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  private static String toString(FormatContext ctx, OptionElement option) {
    StringBuilder sb = new StringBuilder();
    String name = option.getName();
    if (option.isParenthesized()) {
      sb.append("(").append(name).append(")");
    } else {
      sb.append(name);
    }
    Object value = option.getValue();
    switch (option.getKind()) {
      case STRING:
        sb.append(" = \"");
        sb.append(escapeChars(value.toString()));
        sb.append("\"");
        break;
      case BOOLEAN:
      case ENUM:
        sb.append(" = ");
        sb.append(value);
        break;
      case NUMBER:
        sb.append(" = ");
        sb.append(formatNumber(ctx, value));
        break;
      case OPTION:
        sb.append(".");
        // Treat nested options as non-parenthesized always, prevents double parentheses.
        sb.append(toString(ctx, (OptionElement) value));
        break;
      case MAP:
        sb.append(" = {\n");
        formatOptionMap(ctx, sb, (Map<String, Object>) value);
        sb.append('}');
        break;
      case LIST:
        sb.append(" = ");
        appendOptions(ctx, sb, (List<Object>) value);
        break;
      default:
        break;
    }
    return sb.toString();
  }

  private static List<TypeElement> filterTypes(FormatContext ctx, List<TypeElement> types) {
    if (ctx.normalize()) {
      return types.stream()
          .filter(type -> {
            if (type instanceof MessageElement) {
              TypeElementInfo typeInfo = ctx.getType(type.getName(), true);
              // Don't emit synthetic map message
              return typeInfo == null || !typeInfo.isMap();
            } else {
              return true;
            }
          })
          .collect(Collectors.toList());
    } else {
      return types;
    }
  }

  private static void formatOptionMap(
      FormatContext ctx, StringBuilder sb, Map<String, Object> valueMap) {
    if (ctx.normalize()) {
      valueMap = valueMap.entrySet().stream()
          .filter(e -> {
            Object value = e.getValue();
            return !(value instanceof List) || !((List<?>) value).isEmpty();
          })
          .map(e -> {
            String key = e.getKey();
            Object value = e.getValue();
            if (key.startsWith("[") && key.endsWith("]")) {
              // Found an extension field
              String fieldName = key.substring(1, key.length() - 1);
              String resolved = ctx.resolve(ctx::getExtendFieldForFullName, fieldName, true);
              if (resolved != null) {
                return new Pair<>("[" + resolved + "]", value);
              }
            }
            return new Pair<>(key, value);
          })
          .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond,
              (e1, e2) -> e1, TreeMap::new));
    }
    int lastIndex = valueMap.size() - 1;
    int index = 0;
    for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      String endl = index != lastIndex ? "," : "";
      String kv = new StringBuilder()
          .append(key)
          .append(": ")
          .append(formatOptionMapOrListValue(ctx, value))
          .append(endl)
          .toString();
      appendIndented(sb, kv);
      index++;
    }
  }

  @SuppressWarnings("unchecked")
  private static String formatOptionMapOrListValue(FormatContext ctx, Object value) {
    StringBuilder sb = new StringBuilder();
    if (value instanceof String) {
      sb.append("\"");
      sb.append(escapeChars(value.toString()));
      sb.append("\"");
    } else if (value instanceof Map) {
      sb.append("{\n");
      formatOptionMap(ctx, sb, (Map<String, Object>) value);
      sb.append('}');
    } else if (value instanceof List) {
      List<Object> list = (List<Object>) value;
      if (ctx.normalize() && list.size() == 1) {
        sb.append(formatOptionMapOrListValue(ctx, list.get(0)));
      } else {
        sb.append("[\n");
        int lastIndex = list.size() - 1;
        for (int i = 0; i < list.size(); i++) {
          String endl = i != lastIndex ? "," : "";
          String v = new StringBuilder()
              .append(formatOptionMapOrListValue(ctx, list.get(i)))
              .append(endl)
              .toString();
          appendIndented(sb, v);
        }
        sb.append("]");
      }
    } else if (value instanceof OptionElement.OptionPrimitive) {
      OptionElement.OptionPrimitive primitive = (OptionElement.OptionPrimitive) value;
      switch (primitive.getKind()) {
        case BOOLEAN:
        case ENUM:
          sb.append(primitive.getValue());
          break;
        case NUMBER:
          sb.append(formatNumber(ctx, primitive.getValue()));
          break;
        default:
          sb.append(formatOptionMapOrListValue(ctx, primitive.getValue()));
      }
    } else if (value instanceof OptionElement) {
      sb.append(toString(ctx, (OptionElement) value));
    } else {
      sb.append(value);
    }
    return sb.toString();
  }

  private static String toOptionString(FormatContext ctx, OptionElement option) {
    StringBuilder sb = new StringBuilder();
    sb.append("option ")
        .append(toString(ctx, option))
        .append(";\n");
    return sb.toString();
  }

  private static void appendOptions(
      FormatContext ctx, StringBuilder sb, List<?> options) {
    int count = options.size();
    if (count == 1) {
      sb.append('[')
          .append(formatOptionMapOrListValue(ctx, options.get(0)))
          .append(']');
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < count; i++) {
      String endl = i < count - 1 ? "," : "";
      appendIndented(sb, formatOptionMapOrListValue(ctx, options.get(i)) + endl);
    }
    sb.append(']');
  }

  private static Kind toKind(ProtoType protoType) {
    switch (protoType.getSimpleName()) {
      case "bool":
        return OptionElement.Kind.BOOLEAN;
      case "string":
        return OptionElement.Kind.STRING;
      case "bytes":
      case "double":
      case "float":
      case "fixed32":
      case "fixed64":
      case "int32":
      case "int64":
      case "sfixed32":
      case "sfixed64":
      case "sint32":
      case "sint64":
      case "uint32":
      case "uint64":
        return OptionElement.Kind.NUMBER;
      default:
        return OptionElement.Kind.ENUM;
    }
  }

  private static void appendIndented(StringBuilder sb, String value) {
    List<String> lines = Arrays.asList(value.split("\n"));
    if (lines.size() > 1 && lines.get(lines.size() - 1).isEmpty()) {
      lines.remove(lines.size() - 1);
    }
    for (String line : lines) {
      sb.append("  ")
              .append(line)
              .append('\n');
    }
  }

  public static String escapeChars(String input) {
    StringBuilder buffer = new StringBuilder(input.length());
    for (int i = 0; i < input.length(); i++) {
      char curr = input.charAt(i);
      switch (curr) {
        case Ascii.BEL:
          buffer.append("\\a");
          break;
        case Ascii.BS:
          buffer.append("\\b");
          break;
        case Ascii.FF:
          buffer.append("\\f");
          break;
        case Ascii.NL:
          buffer.append("\\n");
          break;
        case Ascii.CR:
          buffer.append("\\r");
          break;
        case Ascii.HT:
          buffer.append("\\t");
          break;
        case Ascii.VT:
          buffer.append("\\v");
          break;
        case '\\':
          buffer.append("\\\\");
          break;
        case '\'':
          buffer.append("\\'");
          break;
        case '\"':
          buffer.append("\\\"");
          break;
        default:
          buffer.append(curr);
      }
    }
    return buffer.toString();
  }

  private static String formatNumber(FormatContext formatContext, Object value) {
    if (formatContext.normalize()) {
      try {
        Number num;
        if (value instanceof Number) {
          num = (Number) value;
        } else {
          num = formatContext.parseNumber(value.toString());
        }
        value = formatContext.formatNumber(num);
      } catch (NumberFormatException e) {
        // ignore, could be -inf or nan for example
      }
    }
    return value.toString();
  }

  private static String resolve(Context ctx, String type) {
    String resolved = ctx.resolve(type, true);
    if (resolved == null) {
      throw new IllegalArgumentException("Could not resolve type: " + type);
    }
    return "." + resolved;
  }

  public static class FormatContext extends Context {
    private boolean ignoreExtensions;
    private boolean normalize;
    private NumberFormat numberFormat;

    public FormatContext(boolean ignoreExtensions, boolean normalize) {
      super();
      this.ignoreExtensions = ignoreExtensions;
      this.normalize = normalize;
    }

    public boolean ignoreExtensions() {
      return ignoreExtensions;
    }

    public boolean normalize() {
      return normalize;
    }

    public String formatNumber(Number number) {
      return numberFormat().format(number);
    }

    public Number parseNumber(String str) {
      return NumberUtils.createNumber(str);
    }

    private NumberFormat numberFormat() {
      if (numberFormat == null) {
        numberFormat = new DecimalFormat();
        numberFormat.setGroupingUsed(false);
      }
      return numberFormat;
    }

    public List<OptionElement> filterOptions(List<OptionElement> options) {
      if (options.isEmpty()) {
        return options;
      }
      if (ignoreExtensions) {
        // Remove custom options
        options = options.stream()
            .filter(o -> !o.isParenthesized() || o.getName().startsWith(CONFLUENT_PREFIX))
            .collect(Collectors.toList());
      }
      if (normalize) {
        options = options.stream()
            // qualify names and transform from Kind.OPTION to Kind.MAP
            .map(o -> {
              if (o.isParenthesized()) {
                String resolved = resolve(this::getExtendFieldForFullName, o.getName(), true);
                if (resolved != null) {
                  return transform(new OptionElement(
                      resolved,
                      o.getKind(),
                      o.getValue(),
                      o.isParenthesized())
                  );
                }
              }
              return o;
            })
            .sorted(Comparator.comparing(OptionElement::getName))
            .collect(Collectors.groupingBy(OptionElement::getName,
                LinkedHashMap::new,  // deterministic order
                Collectors.toList()))
            .entrySet()
            .stream()
            // merge option maps for non-repeated options
            .flatMap(entry -> {
              String name = entry.getKey();
              List<OptionElement> list = entry.getValue();
              ExtendFieldElementInfo fieldInfo = getExtendFieldForFullName(name, true);
              if (fieldInfo != null && !fieldInfo.isRepeated() && list.size() > 0) {
                return Stream.of(list.stream().reduce(ProtobufSchema::merge).get());
              } else {
                return list.stream();
              }
            })
            .collect(Collectors.toList());
      }
      return options;
    }
  }
}
