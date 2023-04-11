load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_squareup_okio_okio",
      artifact = "com.squareup.okio:okio:3.0.0",
      artifact_sha256 = "dcbe63ed43b2c90c325e9e6a0863e2e7605980bff5e728c6de1088be5574979e",
      runtime_deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib_common"
      ],
  )


  import_external(
      name = "com_squareup_okio_okio_jvm",
      artifact = "com.squareup.okio:okio-jvm:3.0.0",
      artifact_sha256 = "be64a0cc1f28ea9cd5c970dd7e7557af72c808d738c495b397bf897c9921e907",
      deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib_common",
          "@org_jetbrains_kotlin_kotlin_stdlib_jdk8"
      ],
  )
