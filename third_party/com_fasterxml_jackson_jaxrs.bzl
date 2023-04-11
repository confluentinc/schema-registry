load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_jackson_jaxrs_jackson_jaxrs_base",
      artifact = "com.fasterxml.jackson.jaxrs:jackson-jaxrs-base:2.14.2",
      artifact_sha256 = "cc0689c44be8d235a643ab58b5d4fb638c8753ce5f8560c13c6fa5f14ac20b55",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
      artifact = "com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:2.13.4",
      artifact_sha256 = "dd58f1e0ea76fe0f50247837ac9809e33b3f9cb340206ad5766422744a08c3f0",
      deps = [
          "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base",
          "@com_fasterxml_jackson_module_jackson_module_jaxb_annotations"
      ],
  )
