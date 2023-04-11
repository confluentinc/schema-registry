load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_eclipse_jetty_jetty_alpn_conscrypt_server",
      artifact = "org.eclipse.jetty:jetty-alpn-conscrypt-server:9.4.48.v20220622",
      artifact_sha256 = "ac2124940ee86e0aba993a74b6cb561607f4163b6ff27a2fd18a46a3c0e7ba4e",
      deps = [
          "@org_conscrypt_conscrypt_openjdk_uber",
          "@org_eclipse_jetty_jetty_alpn_server",
          "@org_eclipse_jetty_jetty_io"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_alpn_java_server",
      artifact = "org.eclipse.jetty:jetty-alpn-java-server:9.4.48.v20220622",
      artifact_sha256 = "10f34f3c9592f82d28a99435e986c4b422a0d2b5ebea2d01543142c08b91267d",
      runtime_deps = [
          "@org_eclipse_jetty_jetty_alpn_server",
          "@org_eclipse_jetty_jetty_io"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_alpn_server",
      artifact = "org.eclipse.jetty:jetty-alpn-server:9.4.48.v20220622",
      artifact_sha256 = "9cca7070635578d3828562c21f2e5360e8333ae67249e5da50b42b4b994c57a7",
      deps = [
          "@org_eclipse_jetty_jetty_server"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_annotations",
      artifact = "org.eclipse.jetty:jetty-annotations:9.4.48.v20220622",
      artifact_sha256 = "ca91b494947932d5ab56a38f8d5153482523498e6f1a5fc8a672851ac3ea1a6f",
      deps = [
          "@javax_annotation_javax_annotation_api",
          "@org_eclipse_jetty_jetty_plus",
          "@org_eclipse_jetty_jetty_webapp",
          "@org_ow2_asm_asm",
          "@org_ow2_asm_asm_commons"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_client",
      artifact = "org.eclipse.jetty:jetty-client:9.4.48.v20220622",
      artifact_sha256 = "7f89fe0900d36b296275999992a6ad76d523be35487d613d8fb56434c34d1d15",
      deps = [
          "@org_eclipse_jetty_jetty_http",
          "@org_eclipse_jetty_jetty_io"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_continuation",
      artifact = "org.eclipse.jetty:jetty-continuation:9.4.48.v20220622",
      artifact_sha256 = "8a4c6f34f0a734012ed4d981f67c56e6be34ca5ffe0996fb4fe0d7f27227a330",
  )


  import_external(
      name = "org_eclipse_jetty_jetty_http",
      artifact = "org.eclipse.jetty:jetty-http:9.4.48.v20220622",
      artifact_sha256 = "c99914804c25288fde0470530411258ee4bab83b69ad764149c816c984f8175e",
      deps = [
          "@org_eclipse_jetty_jetty_io",
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_io",
      artifact = "org.eclipse.jetty:jetty-io:9.4.48.v20220622",
      artifact_sha256 = "4d2f60a0348905a0a70bb266d1eb23a29959281391aba54d17d4a3a0460b8b47",
      deps = [
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_jaas",
      artifact = "org.eclipse.jetty:jetty-jaas:9.4.48.v20220622",
      artifact_sha256 = "6846e8ed3303da130412696172dc79c343dc847c89d9c6cc461c9637b2481069",
      deps = [
          "@org_eclipse_jetty_jetty_security"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_jmx",
      artifact = "org.eclipse.jetty:jetty-jmx:9.4.48.v20220622",
      artifact_sha256 = "341660f9f960df46cc0f87af39e33e69133b09b4f6678ef643d2a396cbd1de6b",
      deps = [
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_jndi",
      artifact = "org.eclipse.jetty:jetty-jndi:9.4.48.v20220622",
      artifact_sha256 = "8cdefa4ebea100e5ec3e7e64cb02bf4e42cdc93dcc41e90374dbf673e1051f3a",
      deps = [
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_plus",
      artifact = "org.eclipse.jetty:jetty-plus:9.4.48.v20220622",
      artifact_sha256 = "358b6e589729670a5ada7e5836f239d8e006a7452e801bd3437b43be1ecbade7",
      deps = [
          "@org_eclipse_jetty_jetty_jndi",
          "@org_eclipse_jetty_jetty_webapp"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_security",
      artifact = "org.eclipse.jetty:jetty-security:9.4.48.v20220622",
      artifact_sha256 = "43039b0f58a156a7f1b9b7750ad82f7fcdd5dba81717b970c381cb1b8618ff73",
      deps = [
          "@org_eclipse_jetty_jetty_server"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_server",
      artifact = "org.eclipse.jetty:jetty-server:9.4.48.v20220622",
      artifact_sha256 = "dbb2b64216b0f10db591319c313979c1389249a196afb9690c022a923c0f0f77",
      deps = [
          "@javax_servlet_javax_servlet_api//:linkable",
          "@org_eclipse_jetty_jetty_http",
          "@org_eclipse_jetty_jetty_io"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_servlet",
      artifact = "org.eclipse.jetty:jetty-servlet:9.4.48.v20220622",
      artifact_sha256 = "eabc36f43fb4080b7d02e1fbcad0b437e035d3adc7bfd7a89b3cdeb22247e682",
      deps = [
          "@org_eclipse_jetty_jetty_security",
          "@org_eclipse_jetty_jetty_util_ajax"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_servlets",
      artifact = "org.eclipse.jetty:jetty-servlets:9.4.48.v20220622",
      artifact_sha256 = "c367bd2020c2aacda24cfc67fd329da85350526e9b0b1fe92dfa44b6f0dbeb40",
      deps = [
          "@org_eclipse_jetty_jetty_continuation",
          "@org_eclipse_jetty_jetty_http",
          "@org_eclipse_jetty_jetty_io",
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_util",
      artifact = "org.eclipse.jetty:jetty-util:9.4.48.v20220622",
      artifact_sha256 = "24cafd449ca4b4bea9c2792b28fc6fe1c43beb628c0c1a0a72ee33afeac82b87",
  )


  import_external(
      name = "org_eclipse_jetty_jetty_util_ajax",
      artifact = "org.eclipse.jetty:jetty-util-ajax:9.4.48.v20220622",
      artifact_sha256 = "b5d4b40be3cf9f48b3d5f8e5918066724a620cb901684939c9f1dd7ec1b930cb",
      deps = [
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_webapp",
      artifact = "org.eclipse.jetty:jetty-webapp:9.4.48.v20220622",
      artifact_sha256 = "bdb33dd7e9a30ea428f301010d08c7f69b37ec75dda340b79a86e95149fec0b2",
      deps = [
          "@org_eclipse_jetty_jetty_servlet",
          "@org_eclipse_jetty_jetty_xml"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_jetty_xml",
      artifact = "org.eclipse.jetty:jetty-xml:9.4.48.v20220622",
      artifact_sha256 = "fe94705c7f49fe56194abfc16050fafc44be0faf691a2e6dca13c5d343a2bea5",
      deps = [
          "@org_eclipse_jetty_jetty_util"
      ],
  )
