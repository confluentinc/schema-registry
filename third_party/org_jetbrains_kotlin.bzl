load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_jetbrains_kotlin_kotlin_reflect",
      artifact = "org.jetbrains.kotlin:kotlin-reflect:1.7.0",
      artifact_sha256 = "d22146070957a44360006837873c51b6602c96a819576b40fdabcd95b7740771",
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_script_runtime",
      artifact = "org.jetbrains.kotlin:kotlin-script-runtime:1.6.0",
      artifact_sha256 = "ddca0f765c416e77a4d8816f3d2df6eda953f61af811737846a22033225a0e57",
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_scripting_common",
      artifact = "org.jetbrains.kotlin:kotlin-scripting-common:1.6.0",
      artifact_sha256 = "16699b070afc4422300c9ed66e81e98b65f7c691faa852b8d44195e509dd6d22",
      deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_scripting_compiler_embeddable",
      artifact = "org.jetbrains.kotlin:kotlin-scripting-compiler-embeddable:1.6.0",
      artifact_sha256 = "2cd1c6f6af69c16b7934d7d8d67c183349b434f450482d7229a9607136fa0447",
      runtime_deps = [
          "@org_jetbrains_kotlin_kotlin_scripting_compiler_impl_embeddable",
          "@org_jetbrains_kotlin_kotlin_stdlib"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_scripting_compiler_impl_embeddable",
      artifact = "org.jetbrains.kotlin:kotlin-scripting-compiler-impl-embeddable:1.6.0",
      artifact_sha256 = "e4bd48906746e4cd19e016445599dca2683a994da06ac38cad383aac48338da8",
      runtime_deps = [
          "@org_jetbrains_kotlin_kotlin_scripting_common",
          "@org_jetbrains_kotlin_kotlin_scripting_jvm",
          "@org_jetbrains_kotlin_kotlin_stdlib"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_scripting_jvm",
      artifact = "org.jetbrains.kotlin:kotlin-scripting-jvm:1.6.0",
      artifact_sha256 = "5f6a7ea274cb6c6c4372094a3572df2a392aa5389f1553b824873d62d6003652",
      deps = [
          "@org_jetbrains_kotlin_kotlin_script_runtime",
          "@org_jetbrains_kotlin_kotlin_scripting_common",
          "@org_jetbrains_kotlin_kotlin_stdlib"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_stdlib",
      artifact = "org.jetbrains.kotlin:kotlin-stdlib:1.6.0",
      artifact_sha256 = "115daea30b0d484afcf2360237b9d9537f48a4a2f03f3cc2a16577dfc6e90342",
      deps = [
          "@org_jetbrains_annotations",
          "@org_jetbrains_kotlin_kotlin_stdlib_common"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_stdlib_common",
      artifact = "org.jetbrains.kotlin:kotlin-stdlib-common:1.5.31",
      artifact_sha256 = "dfa2a18e26b028388ee1968d199bf6f166f737ab7049c25a5e2da614404e22ad",
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_stdlib_jdk7",
      artifact = "org.jetbrains.kotlin:kotlin-stdlib-jdk7:1.5.31",
      deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib"
      ],
  )


  import_external(
      name = "org_jetbrains_kotlin_kotlin_stdlib_jdk8",
      artifact = "org.jetbrains.kotlin:kotlin-stdlib-jdk8:1.5.31",
      artifact_sha256 = "b548f7767aacf029d2417e47440742bd6d3ebede19b60386e23554ce5c4c5fdc",
      deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib",
          "@org_jetbrains_kotlin_kotlin_stdlib_jdk7"
      ],
  )
