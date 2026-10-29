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

import com.google.common.collect.ImmutableSet;
import dev.cel.checker.CelCheckerBuilder;
import dev.cel.common.CelOptions;
import dev.cel.compiler.CelCompilerLibrary;
import dev.cel.runtime.CelInternalRuntimeLibrary;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelRuntimeLibrary;
import dev.cel.runtime.CelStandardFunctions.StandardFunction;
import dev.cel.runtime.RuntimeEquality;

/**
 * Schema Registry's built-in CEL function library. Implements both
 * {@link CelCompilerLibrary} (so {@link BuiltinDeclarations}'s function
 * declarations are added to the type-checker) and {@link CelRuntimeLibrary}
 * (so {@link BuiltinOverload}'s function bindings are added to the runtime).
 *
 * <p>Specifically {@link CelInternalRuntimeLibrary} rather than plain {@link CelRuntimeLibrary},
 * because {@link BuiltinOverload#standardOverrides} needs the runtime's own
 * {@link RuntimeEquality}: it re-registers standard function bindings, and its {@code ==}
 * replacement delegates every non-decimal comparison to the standard implementation. Building one
 * by hand is not an option — {@code RuntimeEquality.create(RuntimeHelpers.create(), options)}
 * yields an instance whose {@code adaptProtoToValue} throws "Not implemented yet", so it fails on
 * any comparison involving a protobuf message. The runtime constructs a {@code
 * ProtoMessageRuntimeEquality} and hands it to internal libraries; receiving that is both correct
 * and less machinery than replicating it.
 *
 * <p>{@link CelInternalRuntimeLibrary} and {@link RuntimeEquality} are cel-java {@code @Internal}
 * types — the only two this repository depends on. {@code CelStandardEqualityTest} is the pin: its
 * {@code protoMessageEquality} case fails outright if this hook stops supplying a message-capable
 * equality, so a cel-java upgrade that changes it breaks the build here rather than silently
 * downstream.
 */
public class BuiltinLibrary
    implements CelCompilerLibrary, CelRuntimeLibrary, CelInternalRuntimeLibrary {

  @Override
  public void setCheckerOptions(CelCheckerBuilder checkerBuilder) {
    checkerBuilder.addFunctionDeclarations(BuiltinDeclarations.create());
  }

  /**
   * Plain-{@link CelRuntimeLibrary} entry point. Reached only if a runtime implementation does not
   * recognize {@link CelInternalRuntimeLibrary}; it registers the namespaced bindings and skips
   * the standard-function overrides, which have no {@link RuntimeEquality} to bind against here.
   */
  @Override
  public void setRuntimeOptions(CelRuntimeBuilder runtimeBuilder) {
    runtimeBuilder.addFunctionBindings(BuiltinOverload.create());
  }

  @Override
  public void setRuntimeOptions(
      CelRuntimeBuilder runtimeBuilder, RuntimeEquality runtimeEquality, CelOptions celOptions) {
    runtimeBuilder
        .addFunctionBindings(BuiltinOverload.create())
        .addFunctionBindings(BuiltinOverload.standardOverrides(celOptions, runtimeEquality));
  }

  /**
   * The standard functions this library takes over, which the caller must exclude from the
   * runtime's standard functions. The exclusion cannot happen here: it is a
   * {@code setStandardFunctions} call, and a second such call silently discards the first, so it
   * has to be made in the same place as any other standard-function subsetting.
   */
  public static ImmutableSet<StandardFunction> overriddenStandardFunctions() {
    return BuiltinOverload.OVERRIDDEN_STANDARD_FUNCTIONS;
  }
}
