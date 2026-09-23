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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.OneofDescriptor;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.CommonConstants;
import io.confluent.kafka.schemaregistry.type.logical.protobuf.ProtoToLogicalTypeConverter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Protobuf location's names as its descriptor spells them: the logical type's names with a
 * {@code value} step added wherever the logical type sees through a {@code flink.wrapped} wrapper.
 * A consumer walking the descriptor by these names reaches the field the location stands for.
 */
final class ProtoNativeNames {

  private static final String ELEMENT = "[]";
  private static final String KEY = "{key}";
  private static final String VALUE = "{value}";
  private static final String PAYLOAD = CommonConstants.FLINK_WRAPPER_FIELD_NAME;

  private final List<String> names = new ArrayList<>();
  private Descriptor message;
  private FieldDescriptor field;
  // A wrapper passed through but not yet stepped into: only a further step enters it.
  private Descriptor wrapper;

  private ProtoNativeNames(Descriptor message) {
    this.message = message;
  }

  /**
   * The native form of {@code logicalNames} under {@code root}, or {@code logicalNames} itself if
   * the descriptor does not have them; in a multi-message file the first name is a top-level
   * message's full name.
   */
  static List<String> of(Descriptor root, boolean multiMessage, List<String> logicalNames) {
    int start = 0;
    Descriptor message = root;
    if (multiMessage) {
      message = topLevel(root, logicalNames.get(0));
      start = 1;
    }
    ProtoNativeNames walk = new ProtoNativeNames(message);
    if (multiMessage) {
      walk.names.add(logicalNames.get(0));
    }
    for (String name : logicalNames.subList(start, logicalNames.size())) {
      if (!walk.step(name)) {
        return logicalNames;
      }
    }
    return Collections.unmodifiableList(walk.names);
  }

  private boolean step(String name) {
    while (wrapper != null) {
      enterPayload();
    }
    if (ELEMENT.equals(name)) {
      if (field == null || !field.isRepeated()) {
        return false;
      }
      names.add(name);
      if (ProtoToLogicalTypeConverter.isFlinkWrapped(field)) {
        wrapper = field.getMessageType();
      } else {
        message = messageOf(field);
      }
      return true;
    }
    if (KEY.equals(name) || VALUE.equals(name)) {
      if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
        return false;
      }
      names.add(name);
      return land(field.getMessageType().findFieldByName(KEY.equals(name) ? "key" : "value"));
    }
    if (message == null) {
      return false;
    }
    for (OneofDescriptor oneof : message.getRealOneofs()) {
      if (oneof.getName().equals(name)) {
        // A oneof is a step in names only; its branches are fields of the same message.
        names.add(name);
        field = null;
        return true;
      }
    }
    names.add(name);
    return land(message.findFieldByName(name));
  }

  private boolean land(FieldDescriptor landed) {
    if (landed == null) {
      return false;
    }
    field = landed;
    message = landed.isMapField() ? null : messageOf(landed);
    if (!landed.isRepeated() && ProtoToLogicalTypeConverter.isFlinkWrapped(landed)) {
      wrapper = landed.getMessageType();
    }
    return true;
  }

  private void enterPayload() {
    Descriptor entered = wrapper;
    wrapper = null;
    names.add(PAYLOAD);
    FieldDescriptor payload = entered.findFieldByName(PAYLOAD);
    if (payload != null && payload.getRealContainingOneof() == null) {
      land(payload);
    } else {
      // The payload is a oneof named value, whose branches are the wrapper's fields.
      field = null;
      message = entered;
    }
  }

  private static Descriptor messageOf(FieldDescriptor field) {
    return field.getJavaType() == FieldDescriptor.JavaType.MESSAGE ? field.getMessageType() : null;
  }

  private static Descriptor topLevel(Descriptor root, String fullName) {
    for (Descriptor candidate : root.getFile().getMessageTypes()) {
      if (candidate.getFullName().equals(fullName)) {
        return candidate;
      }
    }
    return null;
  }
}
