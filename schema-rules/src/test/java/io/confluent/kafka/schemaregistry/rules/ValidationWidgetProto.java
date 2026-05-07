/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules;

@com.google.protobuf.Generated
public final class ValidationWidgetProto extends com.google.protobuf.GeneratedFile {
  private ValidationWidgetProto() {}
  static {
    com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
      com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
      /* major= */ 4,
      /* minor= */ 34,
      /* patch= */ 0,
      /* suffix= */ "",
      "ValidationWidgetProto");
  }
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface ValidationPersonOrBuilder extends
      // @@protoc_insertion_point(interface_extends:io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>int32 age = 1 [(.confluent.field_meta) = { ... }</code>
     * @return The age.
     */
    int getAge();

    /**
     * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();
  }
  /**
   * Protobuf type {@code io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson}
   */
  public static final class ValidationPerson extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson)
      ValidationPersonOrBuilder {
  private static final long serialVersionUID = 0L;
    static {
      com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
        com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
        /* major= */ 4,
        /* minor= */ 34,
        /* patch= */ 0,
        /* suffix= */ "",
        "ValidationPerson");
    }
    // Use ValidationPerson.newBuilder() to construct.
    private ValidationPerson(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }
    private ValidationPerson() {
      name_ = "";
    }

    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
      return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.class, io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.Builder.class);
    }

    public static final int AGE_FIELD_NUMBER = 1;
    private int age_ = 0;
    /**
     * <code>int32 age = 1 [(.confluent.field_meta) = { ... }</code>
     * @return The age.
     */
    @java.lang.Override
    public int getAge() {
      return age_;
    }

    public static final int NAME_FIELD_NUMBER = 2;
    @SuppressWarnings("serial")
    private volatile java.lang.Object name_ = "";
    /**
     * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
     * @return The name.
     */
    @java.lang.Override
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        name_ = s;
        return s;
      }
    }
    /**
     * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
     * @return The bytes for name.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (age_ != 0) {
        output.writeInt32(1, age_);
      }
      if (!com.google.protobuf.GeneratedMessage.isStringEmpty(name_)) {
        com.google.protobuf.GeneratedMessage.writeString(output, 2, name_);
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (age_ != 0) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, age_);
      }
      if (!com.google.protobuf.GeneratedMessage.isStringEmpty(name_)) {
        size += com.google.protobuf.GeneratedMessage.computeStringSize(2, name_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson)) {
        return super.equals(obj);
      }
      io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson other = (io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson) obj;

      if (getAge()
          != other.getAge()) return false;
      if (!getName()
          .equals(other.getName())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + AGE_FIELD_NUMBER;
      hash = (53 * hash) + getAge();
      hash = (37 * hash) + NAME_FIELD_NUMBER;
      hash = (53 * hash) + getName().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseDelimitedWithIOException(PARSER, input);
    }

    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson)
        io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPersonOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.class, io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.Builder.class);
      }

      // Construct using io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.newBuilder()
      private Builder() {

      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);

      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        age_ = 0;
        name_ = "";
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor;
      }

      @java.lang.Override
      public io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson getDefaultInstanceForType() {
        return io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.getDefaultInstance();
      }

      @java.lang.Override
      public io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson build() {
        io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson buildPartial() {
        io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson result = new io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson(this);
        if (bitField0_ != 0) { buildPartial0(result); }
        onBuilt();
        return result;
      }

      private void buildPartial0(io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.age_ = age_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.name_ = name_;
        }
      }

      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson) {
          return mergeFrom((io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson other) {
        if (other == io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson.getDefaultInstance()) return this;
        if (other.getAge() != 0) {
          setAge(other.getAge());
        }
        if (!other.getName().isEmpty()) {
          name_ = other.name_;
          bitField0_ |= 0x00000002;
          onChanged();
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new java.lang.NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 8: {
                age_ = input.readInt32();
                bitField0_ |= 0x00000001;
                break;
              } // case 8
              case 18: {
                name_ = input.readStringRequireUtf8();
                bitField0_ |= 0x00000002;
                break;
              } // case 18
              default: {
                if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                  done = true; // was an endgroup tag
                }
                break;
              } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }
      private int bitField0_;

      private int age_ ;
      /**
       * <code>int32 age = 1 [(.confluent.field_meta) = { ... }</code>
       * @return The age.
       */
      @java.lang.Override
      public int getAge() {
        return age_;
      }
      /**
       * <code>int32 age = 1 [(.confluent.field_meta) = { ... }</code>
       * @param value The age to set.
       * @return This builder for chaining.
       */
      public Builder setAge(int value) {

        age_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>int32 age = 1 [(.confluent.field_meta) = { ... }</code>
       * @return This builder for chaining.
       */
      public Builder clearAge() {
        bitField0_ = (bitField0_ & ~0x00000001);
        age_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          name_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
       * @return The bytes for name.
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) { throw new NullPointerException(); }
        name_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        name_ = getDefaultInstance().getName();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }
      /**
       * <code>string name = 2 [(.confluent.field_meta) = { ... }</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) { throw new NullPointerException(); }
        checkByteStringIsUtf8(value);
        name_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }

      // @@protoc_insertion_point(builder_scope:io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson)
    }

    // @@protoc_insertion_point(class_scope:io.confluent.kafka.schemaregistry.rules.widget.ValidationPerson)
    private static final io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson();
    }

    public static io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<ValidationPerson>
        PARSER = new com.google.protobuf.AbstractParser<ValidationPerson>() {
      @java.lang.Override
      public ValidationPerson parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        Builder builder = newBuilder();
        try {
          builder.mergeFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.setUnfinishedMessage(builder.buildPartial());
        } catch (com.google.protobuf.UninitializedMessageException e) {
          throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
        } catch (java.io.IOException e) {
          throw new com.google.protobuf.InvalidProtocolBufferException(e)
              .setUnfinishedMessage(builder.buildPartial());
        }
        return builder.buildPartial();
      }
    };

    public static com.google.protobuf.Parser<ValidationPerson> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<ValidationPerson> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public io.confluent.kafka.schemaregistry.rules.ValidationWidgetProto.ValidationPerson getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static final com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\026ValidationWidget.proto\022.io.confluent.k" +
      "afka.schemaregistry.rules.widget\032\024conflu" +
      "ent/meta.proto\"\227\001\n\020ValidationPerson\022*\n\003a" +
      "ge\030\001 \001(\005B\035\202D\032\"\030\n\013agePositive\032\tthis >= 0\022" +
      "1\n\004name\030\002 \001(\tB#\202D \"\036\n\014nameNotEmpty\032\016size" +
      "(this) > 0:$\202D!\"\037\n\014ageNotInsane\032\017this.ag" +
      "e <= 150BB\n\'io.confluent.kafka.schemareg" +
      "istry.rulesB\025ValidationWidgetProtoP\000b\006pr" +
      "oto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          io.confluent.protobuf.MetaProto.getDescriptor(),
        });
    internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor =
      getDescriptor().getMessageType(0);
    internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_io_confluent_kafka_schemaregistry_rules_widget_ValidationPerson_descriptor,
        new java.lang.String[] { "Age", "Name", });
    descriptor.resolveAllFeaturesImmutable();
    io.confluent.protobuf.MetaProto.getDescriptor();
    com.google.protobuf.ExtensionRegistry registry =
        com.google.protobuf.ExtensionRegistry.newInstance();
    registry.add(io.confluent.protobuf.MetaProto.fieldMeta);
    registry.add(io.confluent.protobuf.MetaProto.messageMeta);
    com.google.protobuf.Descriptors.FileDescriptor
        .internalUpdateFileDescriptor(descriptor, registry);
  }

  // @@protoc_insertion_point(outer_class_scope)
}
