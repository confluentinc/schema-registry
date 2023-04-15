load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_projectnessie_cel_cel_core",
      artifact = "org.projectnessie.cel:cel-core:0.3.11",
      artifact_sha256 = "a8cba224dde799635adbf4a2aef9390e5b52af51640f22bb2b0c6e3cfd5b56ad",
      deps = [
          "@org_projectnessie_cel_cel_generated_pb"
      ],
      runtime_deps = [
          "@org_agrona_agrona",
          "@org_projectnessie_cel_cel_generated_antlr"
      ],
  )


  import_external(
      name = "org_projectnessie_cel_cel_generated_antlr",
      artifact = "org.projectnessie.cel:cel-generated-antlr:0.3.11",
      artifact_sha256 = "80751781a063e71155b176208e4b67cef6482eaa1f96139baed2e628b509a8a7",
  )


  import_external(
      name = "org_projectnessie_cel_cel_generated_pb",
      artifact = "org.projectnessie.cel:cel-generated-pb:0.3.11",
      artifact_sha256 = "4c26d0c8142e8c2b26ef68b6458739ac9737af97baab76c690ed036a4cc7a2d0",
      deps = [
          "@com_google_protobuf_protobuf_java"
      ],
  )


  import_external(
      name = "org_projectnessie_cel_cel_jackson",
      artifact = "org.projectnessie.cel:cel-jackson:0.3.11",
      artifact_sha256 = "2911ec62d19e5fb9adfdf0a6e5cf3515027576e9fca7eb1611c0c7029c850fac",
      deps = [
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
          "@org_projectnessie_cel_cel_core"
      ],
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_protobuf"
      ],
  )


  import_external(
      name = "org_projectnessie_cel_cel_tools",
      artifact = "org.projectnessie.cel:cel-tools:0.3.11",
      artifact_sha256 = "c75e2d6e50f6e7def1db354c831be0a27d8ff0245f26f559057ad04bb4198bad",
      deps = [
          "@org_projectnessie_cel_cel_core"
      ],
  )
