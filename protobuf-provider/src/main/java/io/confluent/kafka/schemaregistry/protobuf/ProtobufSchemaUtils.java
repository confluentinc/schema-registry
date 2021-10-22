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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Ascii;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import com.squareup.wire.schema.internal.parser.EnumConstantElement;
import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.ExtendElement;
import com.squareup.wire.schema.internal.parser.ExtensionsElement;
import com.squareup.wire.schema.internal.parser.FieldElement;
import com.squareup.wire.schema.internal.parser.GroupElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.OneOfElement;
import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import com.squareup.wire.schema.internal.parser.ReservedElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.util.Arrays;
import java.util.List;

public class ProtobufSchemaUtils {

  private static final ObjectMapper jsonMapper = JacksonMapper.INSTANCE;

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

  public static String toString(ProtoFileElement protoFile) {
    StringBuilder sb = new StringBuilder();
    if (protoFile.getSyntax() != null) {
      sb.append("syntax = \"");
      sb.append(protoFile.getSyntax());
      sb.append("\";\n");
    }
    if (protoFile.getPackageName() != null) {
      sb.append("package ");
      sb.append(protoFile.getPackageName());
      sb.append(";\n");
    }
    if (!protoFile.getImports().isEmpty() || !protoFile.getPublicImports().isEmpty()) {
      sb.append('\n');
      for (String file : protoFile.getImports()) {
        sb.append("import \"");
        sb.append(file);
        sb.append("\";\n");
      }
      for (String file : protoFile.getPublicImports()) {
        sb.append("import public \"");
        sb.append(file);
        sb.append("\";\n");
      }
    }
    if (!protoFile.getOptions().isEmpty()) {
      sb.append('\n');
      for (OptionElement option : protoFile.getOptions()) {
        sb.append(toString(option));
      }
    }
    if (!protoFile.getTypes().isEmpty()) {
      sb.append('\n');
      for (TypeElement typeElement : protoFile.getTypes()) {
        if (typeElement instanceof MessageElement) {
          sb.append(toString((MessageElement) typeElement));
        }
      }
      for (TypeElement typeElement : protoFile.getTypes()) {
        if (typeElement instanceof EnumElement) {
          sb.append(toString((EnumElement) typeElement));
        }
      }
    }
    if (!protoFile.getExtendDeclarations().isEmpty()) {
      sb.append('\n');
      for (ExtendElement extendDeclaration : protoFile.getExtendDeclarations()) {
        sb.append(extendDeclaration.toSchema());
      }
    }
    if (!protoFile.getServices().isEmpty()) {
      sb.append('\n');
      for (ServiceElement service : protoFile.getServices()) {
        sb.append(service.toSchema());
      }
    }
    return sb.toString();
  }

  private static String toString(EnumElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("enum ");
    sb.append(type.getName());
    sb.append(" {");

    if (!type.getOptions().isEmpty() || !type.getConstants().isEmpty()) {
      sb.append('\n');
    }

    if (!type.getOptions().isEmpty()) {
      for (OptionElement option : type.getOptions()) {
        appendIndented(sb, toString(option));
      }
    }
    if (!type.getConstants().isEmpty()) {
      for (EnumConstantElement constant : type.getConstants()) {
        appendIndented(sb, constant.toSchema());
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(MessageElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("message ");
    sb.append(type.getName());
    sb.append(" {");

    if (!type.getReserveds().isEmpty()) {
      sb.append('\n');
      for (ReservedElement reserved : type.getReserveds()) {
        appendIndented(sb, reserved.toSchema());
      }
    }
    if (!type.getOptions().isEmpty()) {
      sb.append('\n');
      for (OptionElement option : type.getOptions()) {
        appendIndented(sb, toString(option));
      }
    }
    if (!type.getFields().isEmpty()) {
      sb.append('\n');
      for (FieldElement field : type.getFields()) {
        appendIndented(sb, field.toSchema());
      }
    }
    if (!type.getOneOfs().isEmpty()) {
      sb.append('\n');
      for (OneOfElement oneOf : type.getOneOfs()) {
        appendIndented(sb, toString(oneOf));
      }
    }
    if (!type.getGroups().isEmpty()) {
      sb.append('\n');
      for (GroupElement group : type.getGroups()) {
        appendIndented(sb, group.toSchema());
      }
    }
    if (!type.getExtensions().isEmpty()) {
      sb.append('\n');
      for (ExtensionsElement extension : type.getExtensions()) {
        appendIndented(sb, extension.toSchema());
      }
    }
    if (!type.getNestedTypes().isEmpty()) {
      sb.append('\n');
      for (TypeElement typeElement : type.getNestedTypes()) {
        if (typeElement instanceof MessageElement) {
          appendIndented(sb, toString((MessageElement) typeElement));
        }
      }
      for (TypeElement typeElement : type.getNestedTypes()) {
        if (typeElement instanceof EnumElement) {
          appendIndented(sb, toString((EnumElement) typeElement));
        }
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(OneOfElement type) {
    StringBuilder sb = new StringBuilder();
    sb.append("oneof ");
    sb.append(type.getName());
    sb.append(" {");

    if (!type.getOptions().isEmpty()) {
      sb.append('\n');
      for (OptionElement option : type.getOptions()) {
        appendIndented(sb, toString(option));
      }
    }
    if (!type.getFields().isEmpty()) {
      sb.append('\n');
      for (FieldElement field : type.getFields()) {
        appendIndented(sb, field.toSchema());
      }
    }
    if (!type.getGroups().isEmpty()) {
      sb.append('\n');
      for (GroupElement group : type.getGroups()) {
        appendIndented(sb, group.toSchema());
      }
    }
    sb.append("}\n");
    return sb.toString();
  }

  private static String toString(OptionElement type) {
    String result;
    if (OptionElement.Kind.STRING == type.getKind()) {
      String name = type.getName();
      String value = escapeChars(type.getValue().toString());
      String formattedName = type.isParenthesized() ? String.format("(%s)", name) : name;
      result = String.format("%s = \"%s\"", formattedName, value);
    } else {
      result = type.toSchema();
    }
    return String.format("option %s;%n", result);
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
          buffer.append("\\\'");
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
}