load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_cbor",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-cbor:2.12.6",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_csv",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-csv:2.13.4",
      artifact_sha256 = "8716827190f48005b5cc7c07820eae484705d28bf0c30bfc92d4b7bc80ae011c",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_protobuf",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-protobuf:2.14.1",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_squareup_protoparser"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_xml",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.13.4",
      artifact_sha256 = "76fbf0cedd51af6a13aba39c27c8c29a5a280dc24ee66577d559e4660d8709ce",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_woodstox_woodstox_core",
          "@org_codehaus_woodstox_stax2_api"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_dataformat_jackson_dataformat_yaml",
      artifact = "com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.14.1",
      artifact_sha256 = "9e157e2625ed855ab73af7915e256f6823993a30982f4923c9ca82bb752a0303",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@org_yaml_snakeyaml"
      ],
  )
