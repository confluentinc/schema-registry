/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rules.cel.builtin;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.projectnessie.cel.common.types.BoolT;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.Types;
import org.projectnessie.cel.common.types.ref.TypeEnum;
import org.projectnessie.cel.interpreter.functions.Overload;

final class BuiltinOverload {

  private static final String OVERLOAD_IS_EMAIL = "isEmail";
  private static final String OVERLOAD_IS_HOSTNAME = "isHostname";
  private static final String OVERLOAD_IS_IPV4 = "isIpv4";
  private static final String OVERLOAD_IS_IPV6 = "isIpv6";
  private static final String OVERLOAD_IS_URI = "isUri";
  private static final String OVERLOAD_IS_URI_REF = "isUriRef";
  private static final String OVERLOAD_IS_UUID = "isUuid";

  static Overload[] create() {
    return new Overload[] {
      isEmail(),
      isHostname(),
      isIpv4(),
      isIpv6(),
      isUri(),
      isUriRef(),
      isUuid(),
    };
  }

  private static Overload isEmail() {
    return Overload.unary(
        OVERLOAD_IS_EMAIL,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_EMAIL, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateEmail(input));
        });
  }

  private static Overload isHostname() {
    return Overload.unary(
        OVERLOAD_IS_HOSTNAME,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_HOSTNAME, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateHostname(input));
        });
  }

  private static Overload isIpv4() {
    return Overload.unary(
        OVERLOAD_IS_IPV4,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_IPV4, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateIpv4(input));
        });
  }

  private static Overload isIpv6() {
    return Overload.unary(
        OVERLOAD_IS_IPV6,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_IPV6, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateIpv6(input));
        });
  }

  private static Overload isUri() {
    return Overload.unary(
        OVERLOAD_IS_URI,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_URI, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUri(input));
        });
  }

  private static Overload isUriRef() {
    return Overload.unary(
        OVERLOAD_IS_URI_REF,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_URI_REF, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUriRef(input));
        });
  }

  private static Overload isUuid() {
    return Overload.unary(
        OVERLOAD_IS_UUID,
        value -> {
          if (value.type().typeEnum() != TypeEnum.String) {
            return Err.noSuchOverload(value, OVERLOAD_IS_UUID, null);
          }
          String input = (String) value.value();
          return input.isEmpty() ? BoolT.False : Types.boolOf(validateUuid(input));
        });
  }

  protected static boolean validateEmail(String input) {
    return EmailValidator.getInstance(false, true).isValid(input);
  }

  protected static boolean validateHostname(String input) {
    return DomainValidator.getInstance(true).isValid(input) && !input.contains("_");
  }

  protected static boolean validateIpv4(String input) {
    return InetAddressValidator.getInstance().isValidInet4Address(input);
  }

  protected static boolean validateIpv6(String input) {
    return InetAddressValidator.getInstance().isValidInet6Address(input);
  }

  protected static boolean validateUri(String input) {
    try {
      URI uri = new URI(input);
      return uri.isAbsolute();
    } catch (URISyntaxException e) {
      return false;
    }
  }

  protected static boolean validateUriRef(String input) {
    try {
      new URI(input);
      return true;
    } catch (URISyntaxException e) {
      return false;
    }
  }

  protected static boolean validateUuid(String input) {
    try {
      UUID.fromString(input);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}