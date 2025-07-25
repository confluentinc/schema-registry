/*
 * Copyright 2024 Confluent Inc.
 * Copyright 2015 protobuf-dynamic developers
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
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FieldOptions.CType;
import com.google.protobuf.DescriptorProtos.FieldOptions.EditionDefault;
import com.google.protobuf.DescriptorProtos.FieldOptions.FeatureSupport;
import com.google.protobuf.DescriptorProtos.FieldOptions.JSType;
import com.google.protobuf.DescriptorProtos.FieldOptions.OptionRetention;
import com.google.protobuf.DescriptorProtos.FieldOptions.OptionTargetType;
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
 * FieldDefinition
 */
public class FieldDefinition {
  // --- public static ---

  public static Builder newBuilder(Context ctx, String fieldName, int tag, String type) {
    return new Builder(fieldName).setNumber(tag).setType(ctx, type);
  }

  // --- public ---

  public String toString() {
    return mFieldType.toString();
  }

  // --- package ---

  FieldDescriptorProto getFieldType() {
    return mFieldType;
  }

  // --- private ---

  private FieldDefinition(FieldDescriptorProto fieldType) {
    mFieldType = fieldType;
  }

  private FieldDescriptorProto mFieldType;

  /**
   * FieldDefinition.Builder
   */
  public static class Builder {
    // --- public ---

    public String getName() {
      return mFieldTypeBuilder.getName();
    }

    public Builder setLabel(String label) {
      FieldDescriptorProto.Label protoLabel = sLabelMap.get(label);
      mFieldTypeBuilder.setLabel(protoLabel);
      return this;
    }

    public Builder setProto3Optional(boolean isProto3Optional) {
      mFieldTypeBuilder.setProto3Optional(isProto3Optional);
      return this;
    }

    public Builder setType(Context ctx, String type) {
      FieldDescriptorProto.Type primType = sTypeMap.get(type);
      if (primType != null) {
        mFieldTypeBuilder.setType(primType);
      } else {
        Pair<String, TypeElementInfo> entry =
                ctx.resolveFull(ctx::getTypeForFullName, type, true);
        if (entry != null) {
          TypeElement elem = entry.getSecond().type();
          if (elem instanceof MessageElement) {
            mFieldTypeBuilder.setType(Type.TYPE_MESSAGE);
          } else if (elem instanceof EnumElement) {
            mFieldTypeBuilder.setType(Type.TYPE_ENUM);
          }
        }
        mFieldTypeBuilder.setTypeName(type);
      }
      return this;
    }

    public Builder setName(String name) {
      mFieldTypeBuilder.setName(name);
      return this;
    }

    public Builder setNumber(int num) {
      mFieldTypeBuilder.setNumber(num);
      return this;
    }

    public Builder setDefaultValue(String defaultValue) {
      mFieldTypeBuilder.setDefaultValue(defaultValue);
      return this;
    }

    public Builder setOneofIndex(int index) {
      mFieldTypeBuilder.setOneofIndex(index);
      return this;
    }

    public Builder setJsonName(String jsonName) {
      mFieldTypeBuilder.setJsonName(jsonName);
      return this;
    }

    public Builder setCtype(CType ctype) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setCtype(ctype);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setPacked(boolean isPacked) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setPacked(isPacked);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setJstype(JSType jstype) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setJstype(jstype);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setDeprecated(boolean isDeprecated) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setDeprecated(isDeprecated);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setDebugRedact(boolean isDebugRedact) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setDebugRedact(isDebugRedact);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setRetention(OptionRetention retention) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setRetention(retention);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder addTargets(List<OptionTargetType> targets) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.addAllTargets(targets);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder addEditionDefaults(List<EditionDefault> editionDefaults) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.addAllEditionDefaults(editionDefaults);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setFeatures(FeatureSet features) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setFeatures(features);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setFeatureSupport(FeatureSupport featureSupport) {
      DescriptorProtos.FieldOptions.Builder optionsBuilder =
          DescriptorProtos.FieldOptions.newBuilder();
      optionsBuilder.setFeatureSupport(featureSupport);
      mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setExtendee(String extendee) {
      mFieldTypeBuilder.setExtendee(extendee);
      return this;
    }

    public Builder setMeta(ProtobufMeta meta) {
      Meta m = toMeta(meta);
      if (m != null) {
        DescriptorProtos.FieldOptions.Builder optionsBuilder =
                DescriptorProtos.FieldOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.fieldMeta, m);
        mFieldTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      return this;
    }

    public FieldDefinition build() {
      return new FieldDefinition(mFieldTypeBuilder.build());
    }

    // --- private ---

    private Builder(String fieldName) {
      mFieldTypeBuilder = FieldDescriptorProto.newBuilder();
      mFieldTypeBuilder.setName(fieldName);
    }

    private FieldDescriptorProto.Builder mFieldTypeBuilder;
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
