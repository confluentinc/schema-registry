load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_swagger_core_v3_swagger_annotations",
      artifact = "io.swagger.core.v3:swagger-annotations:2.1.10",
      artifact_sha256 = "59573c4d6357c2121d40069959879cf008783cc8208dc5123f759b0e6a0077ad",
  )


  import_external(
      name = "io_swagger_core_v3_swagger_core",
      artifact = "io.swagger.core.v3:swagger-core:2.1.10",
      artifact_sha256 = "d59efb1f1fb3d83cda3b73b812cfb65b30eb850767d8b9e9b9339f8552f02d7e",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
          "@io_swagger_core_v3_swagger_annotations",
          "@io_swagger_core_v3_swagger_models",
          "@jakarta_validation_jakarta_validation_api",
          "@jakarta_xml_bind_jakarta_xml_bind_api",
          "@org_apache_commons_commons_lang3",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES javax.validation:validation-api
    # EXCLUDES com.google.guava:guava
  )


  import_external(
      name = "io_swagger_core_v3_swagger_models",
      artifact = "io.swagger.core.v3:swagger-models:2.1.10",
      artifact_sha256 = "a90a71cd7ce02c24258306653d59e1fb5d36cb16ebc4f65a6754527c2cfe53c1",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations"
      ],
  )
