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

import com.google.common.collect.ImmutableList;
import dev.cel.runtime.CelFunctionBinding;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.EmailValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

final class BuiltinOverload {

  private BuiltinOverload() {
  }

  static ImmutableList<CelFunctionBinding> create() {
    return ImmutableList.of(
        unaryString("is_email", BuiltinOverload::validateEmail),
        unaryString("is_hostname", BuiltinOverload::validateHostname),
        unaryString("is_ipv4", BuiltinOverload::validateIpv4),
        unaryString("is_ipv6", BuiltinOverload::validateIpv6),
        unaryString("is_uri", BuiltinOverload::validateUri),
        unaryString("is_uri_ref", BuiltinOverload::validateUriRef),
        unaryString("is_uuid", BuiltinOverload::validateUuid));
  }

  /**
   * Build a unary {@code (string) -> bool} binding that returns {@code false}
   * for empty input and otherwise delegates to {@code predicate}.
   */
  private static CelFunctionBinding unaryString(
      String overloadId, java.util.function.Predicate<String> predicate) {
    return CelFunctionBinding.from(
        overloadId,
        String.class,
        (String input) -> !input.isEmpty() && predicate.test(input));
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
