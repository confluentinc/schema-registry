load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_jackson_module_jackson_module_jaxb_annotations",
      artifact = "com.fasterxml.jackson.module:jackson-module-jaxb-annotations:2.13.4",
      artifact_sha256 = "b23725fd92b783e3ddc149d23f565b9da5bdfc98361be81488ff4e45e6735ba1",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@jakarta_activation_jakarta_activation_api",
          "@jakarta_xml_bind_jakarta_xml_bind_api"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_module_jackson_module_parameter_names",
      artifact = "com.fasterxml.jackson.module:jackson-module-parameter-names:2.14.2",
      artifact_sha256 = "b4e3fbea545a155a14dcb8a65c46b57ad8d0fb9627c84f789858263f05299330",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_module_jackson_module_scala_2_13",
      artifact = "com.fasterxml.jackson.module:jackson-module-scala_2.13:2.13.4",
      artifact_sha256 = "c65346d753fb256b4bc81491ccc99c806231e00bf101c4f82bb8fe790432dd50",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_thoughtworks_paranamer_paranamer",
          "@org_scala_lang_scala_library"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
