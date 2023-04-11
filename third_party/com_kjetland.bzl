load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_kjetland_mbknor_jackson_jsonschema_2_13",
      artifact = "com.kjetland:mbknor-jackson-jsonschema_2.13:1.0.39",
      artifact_sha256 = "f1d0cb875bcb398d10158ae68f962ea83499991445125779b8b999b6af969cd1",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@io_github_classgraph_classgraph",
          "@javax_validation_validation_api",
          "@org_jetbrains_kotlin_kotlin_scripting_compiler_embeddable",
          "@org_scala_lang_scala_library",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.jetbrains.kotlin:kotlin-stdlib
  )
