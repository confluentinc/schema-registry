load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_squareup_wire_wire_runtime_jvm",
      artifact = "com.squareup.wire:wire-runtime-jvm:4.4.3",
      artifact_sha256 = "d3c0cc527e7a3dcaf59e3ae41e382dd0f62dfef3043911a7768e3787dc0b6335",
      deps = [
          "@com_squareup_okio_okio",
          "@org_jetbrains_kotlin_kotlin_stdlib_common",
          "@org_jetbrains_kotlin_kotlin_stdlib_jdk8"
      ],
    # EXCLUDES org.jetbrains.kotlin:kotlin-stdlib
  )


  import_external(
      name = "com_squareup_wire_wire_schema_jvm",
      artifact = "com.squareup.wire:wire-schema-jvm:4.4.3",
      artifact_sha256 = "8c1c0cf41ca426ac018416326b6b2d938b2545a562a661c47ac468239177ce69",
      deps = [
          "@com_google_guava_guava",
          "@com_squareup_javapoet",
          "@com_squareup_kotlinpoet",
          "@com_squareup_okio_okio",
          "@com_squareup_wire_wire_runtime_jvm",
          "@org_jetbrains_kotlin_kotlin_stdlib_common",
          "@org_jetbrains_kotlin_kotlin_stdlib_jdk8"
      ],
    # EXCLUDES org.jetbrains.kotlin:kotlin-stdlib
  )
