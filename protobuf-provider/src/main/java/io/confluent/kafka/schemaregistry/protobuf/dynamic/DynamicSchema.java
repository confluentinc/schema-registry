/*
 * Copyright 2020 Confluent Inc.
 * Copyright 2015 protobuf-dynamic developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.protobuf.dynamic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions.CType;
import com.google.protobuf.DescriptorProtos.FieldOptions.JSType;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileOptions;
import com.google.protobuf.DescriptorProtos.FileOptions.OptimizeMode;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;

import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * DynamicSchema
 */
public class DynamicSchema {
  // --- public static ---

  /**
   * Creates a new dynamic schema builder
   *
   * @return the schema builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Parses a serialized schema descriptor (from input stream; closes the stream)
   *
   * @param schemaDescIn the descriptor input stream
   * @return the schema object
   */
  public static DynamicSchema parseFrom(InputStream schemaDescIn)
      throws DescriptorValidationException, IOException {
    try {
      int len;
      byte[] buf = new byte[4096];
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      while ((len = schemaDescIn.read(buf)) > 0) {
        baos.write(buf, 0, len);
      }
      return parseFrom(baos.toByteArray());
    } finally {
      schemaDescIn.close();
    }
  }

  /**
   * Parses a serialized schema descriptor (from byte array)
   *
   * @param schemaDescBuf the descriptor byte array
   * @return the schema object
   */
  public static DynamicSchema parseFrom(byte[] schemaDescBuf)
      throws DescriptorValidationException, IOException {
    return new DynamicSchema(FileDescriptorSet.parseFrom(schemaDescBuf));
  }

  // --- public ---

  /**
   * Gets the protobuf file descriptor proto
   *
   * @return the file descriptor proto
   */
  public FileDescriptorProto getFileDescriptorProto() {
    return mFileDescSet.getFile(0);
  }

  /**
   * Creates a new dynamic message builder for the given message type
   *
   * @param msgTypeName the message type name
   * @return the message builder (null if not found)
   */
  public DynamicMessage.Builder newMessageBuilder(String msgTypeName) {
    Descriptor msgType = getMessageDescriptor(msgTypeName);
    if (msgType == null) {
      return null;
    }
    return DynamicMessage.newBuilder(msgType);
  }

  /**
   * Gets the protobuf message descriptor for the given message type
   *
   * @param msgTypeName the message type name
   * @return the message descriptor (null if not found)
   */
  public Descriptor getMessageDescriptor(String msgTypeName) {
    Descriptor msgType = mMsgDescriptorMapShort.get(msgTypeName);
    if (msgType == null) {
      msgType = mMsgDescriptorMapFull.get(msgTypeName);
    }
    return msgType;
  }

  /**
   * Gets the enum value for the given enum type and name
   *
   * @param enumTypeName the enum type name
   * @param enumName the enum name
   * @return the enum value descriptor (null if not found)
   */
  public EnumValueDescriptor getEnumValue(String enumTypeName, String enumName) {
    EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
    if (enumType == null) {
      return null;
    }
    return enumType.findValueByName(enumName);
  }

  /**
   * Gets the enum value for the given enum type and number
   *
   * @param enumTypeName the enum type name
   * @param enumNumber the enum number
   * @return the enum value descriptor (null if not found)
   */
  public EnumValueDescriptor getEnumValue(String enumTypeName, int enumNumber) {
    EnumDescriptor enumType = getEnumDescriptor(enumTypeName);
    if (enumType == null) {
      return null;
    }
    return enumType.findValueByNumber(enumNumber);
  }

  /**
   * Gets the protobuf enum descriptor for the given enum type
   *
   * @param enumTypeName the enum type name
   * @return the enum descriptor (null if not found)
   */
  public EnumDescriptor getEnumDescriptor(String enumTypeName) {
    EnumDescriptor enumType = mEnumDescriptorMapShort.get(enumTypeName);
    if (enumType == null) {
      enumType = mEnumDescriptorMapFull.get(enumTypeName);
    }
    return enumType;
  }

  /**
   * Returns the message types registered with the schema
   *
   * @return the set of message type names
   */
  public Set<String> getMessageTypes() {
    return new TreeSet<String>(mMsgDescriptorMapFull.keySet());
  }

  /**
   * Returns the enum types registered with the schema
   *
   * @return the set of enum type names
   */
  public Set<String> getEnumTypes() {
    return new TreeSet<String>(mEnumDescriptorMapFull.keySet());
  }

  /**
   * Serializes the schema
   *
   * @return the serialized schema descriptor
   */
  public byte[] toByteArray() {
    return mFileDescSet.toByteArray();
  }

  /**
   * Returns a string representation of the schema
   *
   * @return the schema string
   */
  public String toString() {
    Set<String> msgTypes = getMessageTypes();
    Set<String> enumTypes = getEnumTypes();
    return "types: " + msgTypes + "\nenums: " + enumTypes + "\n" + mFileDescSet;
  }

  // --- private ---

  private DynamicSchema(FileDescriptorSet fileDescSet) throws DescriptorValidationException {
    mFileDescSet = fileDescSet;
    Map<String, FileDescriptor> fileDescMap = init(fileDescSet);

    Set<String> msgDupes = new HashSet<String>();
    Set<String> enumDupes = new HashSet<String>();
    for (FileDescriptor fileDesc : fileDescMap.values()) {
      for (Descriptor msgType : fileDesc.getMessageTypes()) {
        addMessageType(msgType, null, msgDupes, enumDupes);
      }
      for (EnumDescriptor enumType : fileDesc.getEnumTypes()) {
        addEnumType(enumType, null, enumDupes);
      }
    }

    for (String msgName : msgDupes) {
      mMsgDescriptorMapShort.remove(msgName);
    }
    for (String enumName : enumDupes) {
      mEnumDescriptorMapShort.remove(enumName);
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, FileDescriptor> init(FileDescriptorSet fileDescSet)
      throws DescriptorValidationException {
    // check for dupes
    Set<String> allFdProtoNames = new HashSet<String>();
    for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
      if (allFdProtoNames.contains(fdProto.getName())) {
        throw new IllegalArgumentException("duplicate name: " + fdProto.getName());
      }
      allFdProtoNames.add(fdProto.getName());
    }

    // build FileDescriptors, resolve dependencies (imports) if any
    Map<String, FileDescriptor> resolvedFileDescMap = new HashMap<String, FileDescriptor>();
    while (resolvedFileDescMap.size() < fileDescSet.getFileCount()) {
      for (FileDescriptorProto fdProto : fileDescSet.getFileList()) {
        if (resolvedFileDescMap.containsKey(fdProto.getName())) {
          continue;
        }

        // getDependencyList() signature was changed and broke compatibility in 2.6.1; workaround
        // with reflection
        //List<String> dependencyList = fdProto.getDependencyList();
        List<String> dependencyList = null;
        try {
          Method m = fdProto.getClass().getMethod("getDependencyList", (Class<?>[]) null);
          dependencyList = (List<String>) m.invoke(fdProto, (Object[]) null);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        List<FileDescriptor> resolvedFdList = new ArrayList<FileDescriptor>();
        for (String depName : dependencyList) {
          if (!allFdProtoNames.contains(depName)) {
            throw new IllegalArgumentException("cannot resolve import " + depName + " in " + fdProto
                .getName());
          }
          FileDescriptor fd = resolvedFileDescMap.get(depName);
          if (fd != null) {
            resolvedFdList.add(fd);
          }
        }

        if (resolvedFdList.size() == dependencyList.size()) { // dependencies resolved
          FileDescriptor[] fds = new FileDescriptor[resolvedFdList.size()];
          FileDescriptor fd = FileDescriptor.buildFrom(fdProto, resolvedFdList.toArray(fds));
          resolvedFileDescMap.put(fdProto.getName(), fd);
        }
      }
    }

    return resolvedFileDescMap;
  }

  private void addMessageType(
      Descriptor msgType,
      String scope,
      Set<String> msgDupes,
      Set<String> enumDupes
  ) {
    String msgTypeNameFull = msgType.getFullName();
    String msgTypeNameShort = (scope == null ? msgType.getName() : scope + "." + msgType.getName());

    if (mMsgDescriptorMapFull.containsKey(msgTypeNameFull)) {
      throw new IllegalArgumentException("duplicate name: " + msgTypeNameFull);
    }
    if (mMsgDescriptorMapShort.containsKey(msgTypeNameShort)) {
      msgDupes.add(msgTypeNameShort);
    }

    mMsgDescriptorMapFull.put(msgTypeNameFull, msgType);
    mMsgDescriptorMapShort.put(msgTypeNameShort, msgType);

    for (Descriptor nestedType : msgType.getNestedTypes()) {
      addMessageType(nestedType, msgTypeNameShort, msgDupes, enumDupes);
    }
    for (EnumDescriptor enumType : msgType.getEnumTypes()) {
      addEnumType(enumType, msgTypeNameShort, enumDupes);
    }
  }

  private void addEnumType(EnumDescriptor enumType, String scope, Set<String> enumDupes) {
    String enumTypeNameFull = enumType.getFullName();
    String enumTypeNameShort = (
        scope == null
        ? enumType.getName()
        : scope + "." + enumType.getName());

    if (mEnumDescriptorMapFull.containsKey(enumTypeNameFull)) {
      throw new IllegalArgumentException("duplicate name: " + enumTypeNameFull);
    }
    if (mEnumDescriptorMapShort.containsKey(enumTypeNameShort)) {
      enumDupes.add(enumTypeNameShort);
    }

    mEnumDescriptorMapFull.put(enumTypeNameFull, enumType);
    mEnumDescriptorMapShort.put(enumTypeNameShort, enumType);
  }

  static Meta toMeta(String doc, Map<String, String> params) {
    if (doc == null && params == null) {
      return null;
    }
    Meta.Builder metaBuilder = Meta.newBuilder();
    if (doc != null) {
      metaBuilder.setDoc(doc);
    }
    if (params != null) {
      metaBuilder.putAllParams(params);
    }
    return metaBuilder.build();
  }

  private FileDescriptorSet mFileDescSet;
  private Map<String, Descriptor> mMsgDescriptorMapFull = new HashMap<String, Descriptor>();
  private Map<String, Descriptor> mMsgDescriptorMapShort = new HashMap<String, Descriptor>();
  private Map<String, EnumDescriptor> mEnumDescriptorMapFull = new HashMap<String,
      EnumDescriptor>();
  private Map<String, EnumDescriptor> mEnumDescriptorMapShort = new HashMap<String,
      EnumDescriptor>();

  /**
   * DynamicSchema.Builder
   */
  public static class Builder {
    // --- public ---

    public String getName() {
      return mFileDescProtoBuilder.getName();
    }

    /**
     * Builds a dynamic schema
     *
     * @return the schema object
     */
    public DynamicSchema build() throws DescriptorValidationException {
      FileDescriptorSet.Builder fileDescSetBuilder = FileDescriptorSet.newBuilder();
      fileDescSetBuilder.addFile(mFileDescProtoBuilder.build());
      fileDescSetBuilder.mergeFrom(mFileDescSetBuilder.build());
      return new DynamicSchema(fileDescSetBuilder.build());
    }

    public Builder setSyntax(String syntax) {
      mFileDescProtoBuilder.setSyntax(syntax);
      return this;
    }

    public Builder setName(String name) {
      mFileDescProtoBuilder.setName(name);
      return this;
    }

    public Builder setPackage(String name) {
      mFileDescProtoBuilder.setPackage(name);
      return this;
    }

    public boolean containsMessage(String name) {
      List<DescriptorProto> messages = mFileDescProtoBuilder.getMessageTypeList();
      for (DescriptorProto message : messages) {
        if (message.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    public Builder addMessageDefinition(MessageDefinition msgDef) {
      mFileDescProtoBuilder.addMessageType(msgDef.getMessageType());
      return this;
    }

    public boolean containsEnum(String name) {
      List<EnumDescriptorProto> enums = mFileDescProtoBuilder.getEnumTypeList();
      for (EnumDescriptorProto enumer : enums) {
        if (enumer.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    public Builder addEnumDefinition(EnumDefinition enumDef) {
      mFileDescProtoBuilder.addEnumType(enumDef.getEnumType());
      return this;
    }

    public boolean containsService(String name) {
      List<ServiceDescriptorProto> services = mFileDescProtoBuilder.getServiceList();
      for (ServiceDescriptorProto service : services) {
        if (service.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    public Builder addServiceDefinition(ServiceDefinition serviceDef) {
      mFileDescProtoBuilder.addService(serviceDef.getServiceType());
      return this;
    }

    public Builder addExtendDefinition(
        String extendee,
        String label,
        String type,
        String name,
        int num,
        String defaultVal,
        String jsonName,
        String doc,
        Map<String, String> params,
        CType ctype,
        Boolean isPacked,
        JSType jstype,
        Boolean isDeprecated
    ) {
      FieldDescriptorProto.Builder fieldBuilder = MessageDefinition.getFieldBuilder(label, false,
          type, name, num, defaultVal, jsonName, doc, params, ctype, isPacked, jstype, isDeprecated,
          null);
      fieldBuilder.setExtendee(extendee);
      mFileDescProtoBuilder.addExtension(fieldBuilder.build());
      return this;
    }

    // Note: added
    public Builder addDependency(String dependency) {
      for (int i = 0; i < mFileDescProtoBuilder.getDependencyCount(); i++) {
        if (mFileDescProtoBuilder.getDependency(i).equals(dependency)) {
          return this;
        }
      }
      mFileDescProtoBuilder.addDependency(dependency);
      return this;
    }

    // Note: added
    public Builder addPublicDependency(String dependency) {
      for (int i = 0; i < mFileDescProtoBuilder.getDependencyCount(); i++) {
        if (mFileDescProtoBuilder.getDependency(i).equals(dependency)) {
          mFileDescProtoBuilder.addPublicDependency(i);
          return this;
        }
      }
      mFileDescProtoBuilder.addDependency(dependency);
      mFileDescProtoBuilder.addPublicDependency(mFileDescProtoBuilder.getDependencyCount() - 1);
      return this;
    }

    // Note: added
    public Builder setJavaPackage(String javaPackage) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaPackage(javaPackage);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setJavaOuterClassname(String javaOuterClassname) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaOuterClassname(javaOuterClassname);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setJavaMultipleFiles(boolean javaMultipleFiles) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaMultipleFiles(javaMultipleFiles);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setJavaGenerateEqualsAndHash(boolean javaGenerateEqualsAndHash) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaGenerateEqualsAndHash(javaGenerateEqualsAndHash);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setJavaStringCheckUtf8(boolean javaStringCheckUtf8) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaStringCheckUtf8(javaStringCheckUtf8);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setOptimizeFor(OptimizeMode optimizeFor) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setOptimizeFor(optimizeFor);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setGoPackage(String goPackage) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setGoPackage(goPackage);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setCcGenericServices(boolean ccGenericServices) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setCcGenericServices(ccGenericServices);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setJavaGenericServices(boolean javaGenericServices) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setJavaGenericServices(javaGenericServices);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setPyGenericServices(boolean pyGenericServices) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setPyGenericServices(pyGenericServices);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setPhpGenericServices(boolean phpGenericServices) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setPhpGenericServices(phpGenericServices);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setDeprecated(boolean isDeprecated) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setDeprecated(isDeprecated);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setCcEnableArenas(boolean ccEnableArenas) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setCcEnableArenas(ccEnableArenas);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setObjcClassPrefix(String objcClassPrefix) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setObjcClassPrefix(objcClassPrefix);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setCsharpNamespace(String csharpNamespace) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setCsharpNamespace(csharpNamespace);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setSwiftPrefix(String swiftPrefix) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setSwiftPrefix(swiftPrefix);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setPhpClassPrefix(String phpClassPrefix) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setPhpClassPrefix(phpClassPrefix);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setPhpNamespace(String phpNamespace) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setPhpNamespace(phpNamespace);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setPhpMetadataNamespace(String phpMetadataNamespace) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setPhpMetadataNamespace(phpMetadataNamespace);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setRubyPackage(String rubyPackage) {
      FileOptions.Builder optionsBuilder =
          DescriptorProtos.FileOptions.newBuilder();
      optionsBuilder.setRubyPackage(rubyPackage);
      mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    // Note: added
    public Builder setMeta(String doc, Map<String, String> params) {
      Meta meta = toMeta(doc, params);
      if (meta != null) {
        DescriptorProtos.FileOptions.Builder optionsBuilder =
                DescriptorProtos.FileOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.fileMeta, meta);
        mFileDescProtoBuilder.mergeOptions(optionsBuilder.build());
      }
      return this;
    }

    // Note: changed
    public Builder addSchema(DynamicSchema schema) {
      for (FileDescriptorProto file : schema.mFileDescSet.getFileList()) {
        if (!containsFile(file.getName())) {
          mFileDescSetBuilder.addFile(file);
        }
      }
      return this;
    }

    // --- private ---

    private Builder() {
      mFileDescProtoBuilder = FileDescriptorProto.newBuilder();
      mFileDescSetBuilder = FileDescriptorSet.newBuilder();
    }

    // Note: added
    private boolean containsFile(String name) {
      List<FileDescriptorProto> files = mFileDescSetBuilder.getFileList();
      for (FileDescriptorProto file : files) {
        if (file.getName().equals(name)) {
          return true;
        }
      }
      return false;
    }

    private FileDescriptorProto.Builder mFileDescProtoBuilder;
    private FileDescriptorSet.Builder mFileDescSetBuilder;
  }
}
