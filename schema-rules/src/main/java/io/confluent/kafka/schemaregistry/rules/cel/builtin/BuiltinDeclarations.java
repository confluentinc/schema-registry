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

import com.google.api.expr.v1alpha1.Decl;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.projectnessie.cel.checker.Decls;

final class BuiltinDeclarations {

  static List<Decl> create() {
    List<Decl> decls = new ArrayList<>();

    decls.add(
        Decls.newFunction(
            "isEmail",
            Decls.newInstanceOverload(
                "is_email", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isHostname",
            Decls.newInstanceOverload(
                "is_hostname", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isIpv4",
            Decls.newInstanceOverload(
                "is_ipv4", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isIpv6",
            Decls.newInstanceOverload(
                "is_ipv6", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUriRef",
            Decls.newInstanceOverload(
                "is_uri_ref", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUri",
            Decls.newInstanceOverload(
                "is_uri", Collections.singletonList(Decls.String), Decls.Bool)));

    decls.add(
        Decls.newFunction(
            "isUuid",
            Decls.newInstanceOverload(
                "is_uuid", Collections.singletonList(Decls.String), Decls.Bool)));

    return Collections.unmodifiableList(decls);
  }
}
