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
import com.google.common.collect.ImmutableSet;
import dev.cel.checker.CelCheckerBuilder;
import dev.cel.common.CelOptions;
import dev.cel.compiler.CelCompilerLibrary;
import dev.cel.runtime.CelFunctionBinding;
import dev.cel.runtime.CelRuntimeBuilder;
import dev.cel.runtime.CelRuntimeLibrary;
import dev.cel.runtime.CelStandardFunctions.StandardFunction;

/**
 * Schema Registry's built-in CEL function library. Implements both
 * {@link CelCompilerLibrary} (so {@link BuiltinDeclarations}'s function
 * declarations are added to the type-checker) and {@link CelRuntimeLibrary}
 * (so {@link BuiltinOverload}'s function bindings are added to the runtime).
 */
public class BuiltinLibrary implements CelCompilerLibrary, CelRuntimeLibrary {

  @Override
  public void setCheckerOptions(CelCheckerBuilder checkerBuilder) {
    checkerBuilder.addFunctionDeclarations(BuiltinDeclarations.create());
  }

  @Override
  public void setRuntimeOptions(CelRuntimeBuilder runtimeBuilder) {
    runtimeBuilder.addFunctionBindings(BuiltinOverload.create());
  }

  /**
   * The standard functions this library extends, which the caller must exclude from the
   * runtime's standard functions before registering {@link #standardOverrides}.
   */
  public static ImmutableSet<StandardFunction> overriddenStandardFunctions() {
    return BuiltinOverload.OVERRIDDEN_STANDARD_FUNCTIONS;
  }

  /**
   * Bindings for the standard function names this library extends, regrouped with the standard
   * library's own bindings so the planner runtime's name-keyed dispatch sees all of them. Not
   * added by {@link #setRuntimeOptions}, because the exclusion above has to happen in the same
   * place as any other standard-function subsetting: a second setStandardFunctions call would
   * silently discard the first.
   */
  public static ImmutableList<CelFunctionBinding> standardOverrides(CelOptions celOptions) {
    return BuiltinOverload.standardOverrides(celOptions);
  }
}
