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

package io.confluent.kafka.schemaregistry.protobuf.dynamic;

import static io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema.toMeta;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions.CType;
import com.google.protobuf.DescriptorProtos.FieldOptions.JSType;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;

import com.squareup.wire.schema.internal.parser.EnumElement;
import com.squareup.wire.schema.internal.parser.MessageElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.ProtobufMeta;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context;
import io.confluent.kafka.schemaregistry.protobuf.diff.Context.TypeElementInfo;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kotlin.Pair;

/**
 * MessageDefinition
 */
public class MessageDefinition {
  // --- public static ---

  public static Builder newBuilder(String msgTypeName) {
    return new Builder(msgTypeName);
  }

  // --- public ---

  public String toString() {
    return mMsgType.toString();
  }

  // --- package ---

  DescriptorProto getMessageType() {
    return mMsgType;
  }

  // --- private ---

  private MessageDefinition(DescriptorProto msgType) {
    mMsgType = msgType;
  }

  private DescriptorProto mMsgType;

  /**
   * MessageDefinition.Builder
   */
  public static class Builder {
    // --- public ---

    public String getName() {
      return mMsgTypeBuilder.getName();
    }

    public Builder addField(
        Context ctx,
        String label,
        String type,
        String name,
        int num,
        String defaultVal,
        ProtobufMeta meta
    ) {
      return addField(ctx, label, type, name, num, defaultVal,
          null, meta, null, null, null, null);
    }

    public Builder addField(
        Context ctx,
        String label,
        String type,
        String name,
        int num,
        String defaultVal,
        String jsonName,
        ProtobufMeta meta,
        CType ctype,
        Boolean isPacked,
        JSType jstype,
        Boolean isDeprecated
    ) {
      doAddField(ctx, label, false, type, name, num,
              defaultVal, jsonName, meta, ctype, isPacked, jstype, isDeprecated, null);
      return this;
    }

    public OneofBuilder addOneof(String oneofName) {
      mMsgTypeBuilder.addOneofDecl(OneofDescriptorProto.newBuilder().setName(oneofName).build());
      return new OneofBuilder(this, mOneofIndex++);
    }

    public boolean containsMessage(String name) {
      List<DescriptorProto> messages = mMsgTypeBuilder.getNestedTypeList();
      for (DescriptorProto message : messages) {
        if (message.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    public Builder addMessageDefinition(MessageDefinition msgDef) {
      mMsgTypeBuilder.addNestedType(msgDef.getMessageType());
      return this;
    }

    public boolean containsEnum(String name) {
      List<EnumDescriptorProto> enums = mMsgTypeBuilder.getEnumTypeList();
      for (EnumDescriptorProto enumer : enums) {
        if (enumer.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    public Builder addEnumDefinition(EnumDefinition enumDef) {
      mMsgTypeBuilder.addEnumType(enumDef.getEnumType());
      return this;
    }

    // Note: added
    public Builder addReservedName(String reservedName) {
      mMsgTypeBuilder.addReservedName(reservedName);
      return this;
    }

    // Note: added
    public Builder addReservedRange(int start, int end) {
      DescriptorProto.ReservedRange.Builder rangeBuilder =
          DescriptorProto.ReservedRange.newBuilder();
      rangeBuilder.setStart(start).setEnd(end);
      mMsgTypeBuilder.addReservedRange(rangeBuilder.build());
      return this;
    }

    public Builder addExtensionRange(int start, int end) {
      DescriptorProto.ExtensionRange.Builder rangeBuilder =
          DescriptorProto.ExtensionRange.newBuilder();
      rangeBuilder.setStart(start).setEnd(end);
      mMsgTypeBuilder.addExtensionRange(rangeBuilder.build());
      return this;
    }

    public Builder addExtendDefinition(
        Context ctx,
        String extendee,
        String label,
        String type,
        String name,
        int num,
        String defaultVal,
        String jsonName,
        ProtobufMeta meta,
        CType ctype,
        Boolean isPacked,
        JSType jstype,
        Boolean isDeprecated
    ) {
      FieldDescriptorProto.Builder fieldBuilder = MessageDefinition.getFieldBuilder(ctx, label,
          false, type, name, num, defaultVal, jsonName, meta, ctype, isPacked, jstype, isDeprecated,
          null);
      fieldBuilder.setExtendee(extendee);
      mMsgTypeBuilder.addExtension(fieldBuilder.build());
      return this;
    }

    // Note: added
    public Builder setNoStandardDescriptorAccessor(boolean noStandardDescriptorAccessor) {
      DescriptorProtos.MessageOptions.Builder optionsBuilder =
          DescriptorProtos.MessageOptions.newBuilder();
      optionsBuilder.setNoStandardDescriptorAccessor(noStandardDescriptorAccessor);
      mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setDeprecated(boolean isDeprecated) {
      DescriptorProtos.MessageOptions.Builder optionsBuilder =
          DescriptorProtos.MessageOptions.newBuilder();
      optionsBuilder.setDeprecated(isDeprecated);
      mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setMapEntry(boolean mapEntry) {
      DescriptorProtos.MessageOptions.Builder optionsBuilder =
          DescriptorProtos.MessageOptions.newBuilder();
      optionsBuilder.setMapEntry(mapEntry);
      mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setMeta(ProtobufMeta meta) {
      Meta m = toMeta(meta);
      if (m != null) {
        DescriptorProtos.MessageOptions.Builder optionsBuilder =
                DescriptorProtos.MessageOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.messageMeta, m);
        mMsgTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      return this;
    }

    public MessageDefinition build() {
      return new MessageDefinition(mMsgTypeBuilder.build());
    }

    // --- private ---

    private Builder(String msgTypeName) {
      mMsgTypeBuilder = DescriptorProto.newBuilder();
      mMsgTypeBuilder.setName(msgTypeName);
    }

    private void doAddField(
        Context ctx,
        String label,
        boolean isProto3Optional,
        String type,
        String name,
        int num,
        String defaultVal,
        String jsonName,
        ProtobufMeta meta,
        CType ctype,
        Boolean isPacked,
        JSType jstype,
        Boolean isDeprecated,
        OneofBuilder oneofBuilder
    ) {
      FieldDescriptorProto.Builder fieldBuilder = getFieldBuilder(ctx, label, isProto3Optional,
          type, name, num, defaultVal, jsonName, meta, ctype, isPacked, jstype, isDeprecated,
          oneofBuilder);
      mMsgTypeBuilder.addField(fieldBuilder.build());
    }

    private DescriptorProto.Builder mMsgTypeBuilder;
    private int mOneofIndex = 0;
  }

  /**
   * MessageDefinition.OneofBuilder
   */
  public static class OneofBuilder {
    // --- public ---

    public OneofBuilder addField(
        Context ctx,
        String type,
        String name,
        int num,
        String defaultVal,
        ProtobufMeta meta) {
      return addField(ctx, false, type, name, num, defaultVal, null, meta, null, null, false);
    }

    public OneofBuilder addField(
        Context ctx,
        boolean isProto3Optional,
        String type,
        String name,
        int num,
        String defaultVal,
        ProtobufMeta meta) {
      return addField(
          ctx, isProto3Optional, type, name, num, defaultVal, null, meta, null, null, false);
    }

    public OneofBuilder addField(
        Context ctx,
        boolean isProto3Optional,
        String type,
        String name,
        int num,
        String defaultVal,
        String jsonName,
        ProtobufMeta meta,
        CType ctype,
        JSType jstype,
        Boolean deprecated
    ) {
      mMsgBuilder.doAddField(
          ctx,
          "optional",
          isProto3Optional,
          type,
          name,
          num,
          defaultVal,
          jsonName,
          meta,
          ctype,
          null,
          jstype,
          deprecated,
          this
      );
      return this;
    }

    public MessageDefinition.Builder msgDefBuilder() {
      return mMsgBuilder;
    }

    public int getIdx() {
      return mIdx;
    }

    // --- private ---

    private OneofBuilder(MessageDefinition.Builder msgBuilder, int oneofIdx) {
      mMsgBuilder = msgBuilder;
      mIdx = oneofIdx;
    }

    private MessageDefinition.Builder mMsgBuilder;
    private int mIdx;
  }

  public static FieldDescriptorProto.Builder getFieldBuilder(
      Context ctx,
      String label,
      boolean isProto3Optional,
      String type,
      String name,
      int num,
      String defaultVal,
      String jsonName,
      ProtobufMeta meta,
      CType ctype,
      Boolean isPacked,
      JSType jstype,
      Boolean isDeprecated,
      OneofBuilder oneofBuilder
  ) {
    FieldDescriptorProto.Label protoLabel = sLabelMap.get(label);
    FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder();
    // Note: changed
    if (label != null) {
      fieldBuilder.setLabel(protoLabel);
    }
    if (isProto3Optional) {
      fieldBuilder.setProto3Optional(isProto3Optional);
    }
    FieldDescriptorProto.Type primType = sTypeMap.get(type);
    if (primType != null) {
      fieldBuilder.setType(primType);
    } else {
      Pair<String, TypeElementInfo> entry =
          ctx.resolveFull(ctx::getTypeForFullName, type, true);
      if (entry != null) {
        TypeElement elem = entry.getSecond().type();
        if (elem instanceof MessageElement) {
          fieldBuilder.setType(Type.TYPE_MESSAGE);
        } else if (elem instanceof EnumElement) {
          fieldBuilder.setType(Type.TYPE_ENUM);
        }
      }
      fieldBuilder.setTypeName(type);
    }
    fieldBuilder.setName(name).setNumber(num);
    if (defaultVal != null) {
      fieldBuilder.setDefaultValue(defaultVal);
    }
    if (oneofBuilder != null) {
      fieldBuilder.setOneofIndex(oneofBuilder.getIdx());
    }
    if (jsonName != null) {
      fieldBuilder.setJsonName(jsonName);
    }
    if (ctype != null) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setCtype(ctype);
      fieldBuilder.mergeOptions(optionsBuilder.build());
    }
    if (isPacked != null) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setPacked(isPacked);
      fieldBuilder.mergeOptions(optionsBuilder.build());
    }
    if (jstype != null) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setJstype(jstype);
      fieldBuilder.mergeOptions(optionsBuilder.build());
    }
    if (isDeprecated != null) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setDeprecated(isDeprecated);
      fieldBuilder.mergeOptions(optionsBuilder.build());
    }
    setFieldMeta(fieldBuilder, meta);
    return fieldBuilder;
  }

  // --- private static ---

  private static void setFieldMeta(
      FieldDescriptorProto.Builder fieldBuilder, ProtobufMeta meta) {
    Meta m = toMeta(meta);
    if (m != null) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setExtension(MetaProto.fieldMeta, m);
      fieldBuilder.mergeOptions(optionsBuilder.build());
    }
  }

  private static Map<String, FieldDescriptorProto.Type> sTypeMap;
  private static Map<String, FieldDescriptorProto.Label> sLabelMap;

  static {
    sTypeMap = new HashMap<String, FieldDescriptorProto.Type>();
    sTypeMap.put("double", FieldDescriptorProto.Type.TYPE_DOUBLE);
    sTypeMap.put("float", FieldDescriptorProto.Type.TYPE_FLOAT);
    sTypeMap.put("int32", FieldDescriptorProto.Type.TYPE_INT32);
    sTypeMap.put("int64", FieldDescriptorProto.Type.TYPE_INT64);
    sTypeMap.put("uint32", FieldDescriptorProto.Type.TYPE_UINT32);
    sTypeMap.put("uint64", FieldDescriptorProto.Type.TYPE_UINT64);
    sTypeMap.put("sint32", FieldDescriptorProto.Type.TYPE_SINT32);
    sTypeMap.put("sint64", FieldDescriptorProto.Type.TYPE_SINT64);
    sTypeMap.put("fixed32", FieldDescriptorProto.Type.TYPE_FIXED32);
    sTypeMap.put("fixed64", FieldDescriptorProto.Type.TYPE_FIXED64);
    sTypeMap.put("sfixed32", FieldDescriptorProto.Type.TYPE_SFIXED32);
    sTypeMap.put("sfixed64", FieldDescriptorProto.Type.TYPE_SFIXED64);
    sTypeMap.put("bool", FieldDescriptorProto.Type.TYPE_BOOL);
    sTypeMap.put("string", FieldDescriptorProto.Type.TYPE_STRING);
    sTypeMap.put("bytes", FieldDescriptorProto.Type.TYPE_BYTES);
    //sTypeMap.put("enum", FieldDescriptorProto.Type.TYPE_ENUM);
    //sTypeMap.put("message", FieldDescriptorProto.Type.TYPE_MESSAGE);
    //sTypeMap.put("group", FieldDescriptorProto.Type.TYPE_GROUP);

    sLabelMap = new HashMap<String, FieldDescriptorProto.Label>();
    sLabelMap.put("optional", FieldDescriptorProto.Label.LABEL_OPTIONAL);
    sLabelMap.put("required", FieldDescriptorProto.Label.LABEL_REQUIRED);
    sLabelMap.put("repeated", FieldDescriptorProto.Label.LABEL_REPEATED);
  }
}
