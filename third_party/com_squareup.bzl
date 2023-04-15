load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_squareup_javapoet",
      artifact = "com.squareup:javapoet:1.13.0",
      artifact_sha256 = "4c7517e848a71b36d069d12bb3bf46a70fd4cda3105d822b0ed2e19c00b69291",
  )


  import_external(
      name = "com_squareup_kotlinpoet",
      artifact = "com.squareup:kotlinpoet:1.12.0",
      artifact_sha256 = "8e3f7849cdfb5376c87aca4cd40a6b96fbb02ddf060b6211099f5d15211171ce",
      deps = [
          "@org_jetbrains_kotlin_kotlin_stdlib_jdk8"
      ],
      runtime_deps = [
          "@org_jetbrains_kotlin_kotlin_reflect"
      ],
  )


  import_external(
      name = "com_squareup_protoparser",
      artifact = "com.squareup:protoparser:4.0.3",
      artifact_sha256 = "3fb82ea4d0b8c9cd4858c8e42d0355028b696ce99c4c8b89568f8a74d63f9855",
  )
