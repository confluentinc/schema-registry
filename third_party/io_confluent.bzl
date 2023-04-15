load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_confluent_common_config",
      artifact = "io.confluent:common-config:7.5.0-434",
      artifact_sha256 = "ca215b03fed2849f36d5331c6a6ec1caf95fbe7d9d4c2b66113a05f2bd647be4",
      deps = [
          "@io_confluent_common_utils",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "io_confluent_common_utils",
      artifact = "io.confluent:common-utils:7.5.0-434",
      artifact_sha256 = "09c2f5e3d4c4451ec283d2dd2745032bfa455692462789659e343386aa66dab1",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "io_confluent_logredactor",
      artifact = "io.confluent:logredactor:1.0.11",
      artifact_sha256 = "d878b66b9ddc9bef509f0a5f273643a70ef0c325e304f5107851944c316aef4d",
      deps = [
          "@com_eclipsesource_minimal_json_minimal_json",
          "@com_google_re2j_re2j",
          "@io_confluent_logredactor_metrics"
      ],
  )


  import_external(
      name = "io_confluent_logredactor_metrics",
      artifact = "io.confluent:logredactor-metrics:1.0.11",
      artifact_sha256 = "50e050c892861cce94930d81fd6cfe48acf5e3038cc2479b293559d5f448b7a2",
  )


  import_external(
      name = "io_confluent_rest_utils",
      artifact = "io.confluent:rest-utils:7.5.0-423",
      artifact_sha256 = "458af3c4af5d1baf3c4c71a5a078da7e9d23e7f596902928af3c0d70e5f3c5e1",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_base",
          "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
          "@com_google_guava_guava",
          "@io_confluent_common_utils",
          "@javax_activation_activation",
          "@javax_xml_bind_jaxb_api",
          "@org_apache_kafka_kafka_clients",
          "@org_conscrypt_conscrypt_openjdk_uber",
          "@org_eclipse_jetty_http2_http2_server",
          "@org_eclipse_jetty_jetty_alpn_conscrypt_server",
          "@org_eclipse_jetty_jetty_alpn_server",
          "@org_eclipse_jetty_jetty_jaas",
          "@org_eclipse_jetty_jetty_jmx",
          "@org_eclipse_jetty_jetty_server",
          "@org_eclipse_jetty_jetty_servlet",
          "@org_eclipse_jetty_jetty_servlets",
          "@org_eclipse_jetty_websocket_javax_websocket_server_impl",
          "@org_glassfish_jersey_containers_jersey_container_servlet",
          "@org_glassfish_jersey_ext_jersey_bean_validation",
          "@org_glassfish_jersey_inject_jersey_hk2",
          "@org_hibernate_validator_hibernate_validator"
      ],
      runtime_deps = [
          "@org_eclipse_jetty_jetty_alpn_java_server"
      ],
  )
