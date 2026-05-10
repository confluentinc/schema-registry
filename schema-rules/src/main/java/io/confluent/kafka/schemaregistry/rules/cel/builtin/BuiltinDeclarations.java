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
import dev.cel.common.CelFunctionDecl;
import dev.cel.common.CelOverloadDecl;
import dev.cel.common.types.SimpleType;
import java.util.List;

final class BuiltinDeclarations {

  private BuiltinDeclarations() {
  }

  static List<CelFunctionDecl> create() {
    return ImmutableList.of(
        member("isEmail", "is_email"),
        member("isHostname", "is_hostname"),
        member("isIpv4", "is_ipv4"),
        member("isIpv6", "is_ipv6"),
        member("isUriRef", "is_uri_ref"),
        member("isUri", "is_uri"),
        member("isUuid", "is_uuid"));
  }

  /**
   * Build a member-style function declaration: {@code STRING.isFoo() -> bool}.
   */
  private static CelFunctionDecl member(String functionName, String overloadId) {
    return CelFunctionDecl.newFunctionDeclaration(
        functionName,
        CelOverloadDecl.newMemberOverload(
            overloadId, SimpleType.BOOL, SimpleType.STRING));
  }
}
