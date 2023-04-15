load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_kafka_connect_api",
      artifact = "org.apache.kafka:connect-api:7.5.0-154-ccs",
      artifact_sha256 = "0e7fadc815cbf2e413f3feec2db6a3659a55221f9325c67b52c9f00ffb899d58",
      deps = [
          "@org_apache_kafka_kafka_clients"
      ],
      runtime_deps = [
          "@javax_ws_rs_javax_ws_rs_api",
          "@org_slf4j_slf4j_api"
      ],
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_connect_file",
      artifact = "org.apache.kafka:connect-file:7.5.0-154-ccs",
      artifact_sha256 = "4034c51e6c28c982eea155e1b33b653df8bae44657b0a03e8ad8603fd3c09836",
      runtime_deps = [
          "@org_apache_kafka_connect_api",
          "@org_slf4j_slf4j_api"
      ],
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_connect_json",
      artifact = "org.apache.kafka:connect-json:7.5.0-154-ccs",
      artifact_sha256 = "0377d22a7d4677d72f079f0309a9c5570f527cec0b9ae3d002c178ed9836927a",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
          "@org_apache_kafka_connect_api"
      ],
      runtime_deps = [
          "@org_slf4j_slf4j_api"
      ],
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_connect_runtime",
      artifact = "org.apache.kafka:connect-runtime:7.5.0-154-ccs",
      artifact_sha256 = "9bb63f4a587fb42d5399ac79f85815c27ef257c4fa5bad2714db50f66b521aaf",
      deps = [
          "@ch_qos_reload4j_reload4j",
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
          "@javax_activation_activation",
          "@javax_xml_bind_jaxb_api",
          "@org_apache_kafka_connect_api",
          "@org_apache_kafka_connect_json",
          "@org_apache_kafka_connect_transforms",
          "@org_apache_kafka_kafka_clients",
          "@org_eclipse_jetty_jetty_client",
          "@org_eclipse_jetty_jetty_server",
          "@org_eclipse_jetty_jetty_servlet",
          "@org_eclipse_jetty_jetty_servlets",
          "@org_glassfish_jersey_containers_jersey_container_servlet",
          "@org_glassfish_jersey_inject_jersey_hk2"
      ],
      runtime_deps = [
          "@io_swagger_core_v3_swagger_annotations",
          "@org_apache_kafka_kafka_tools",
          "@org_apache_maven_maven_artifact",
          "@org_bitbucket_b_c_jose4j",
          "@org_reflections_reflections",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.slf4j:slf4j-log4j12
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_connect_transforms",
      artifact = "org.apache.kafka:connect-transforms:7.5.0-154-ccs",
      artifact_sha256 = "41f6d0f43bc20ee59ba72f522e8dd1c6848cb3aac482fe225aa6870c9dc0a94b",
      deps = [
          "@org_apache_kafka_connect_api"
      ],
      runtime_deps = [
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_kafka_kafka_2_13",
      artifact = "org.apache.kafka:kafka_2.13:7.5.0-154-ccs",
      artifact_sha256 = "d3cd5c6d52ae2822a9ea9af861124a9ecc11cc0481934213c556be9d211e193b",
      deps = [
          "@org_apache_kafka_kafka_clients",
          "@org_scala_lang_scala_library"
      ],
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_csv",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
          "@com_fasterxml_jackson_module_jackson_module_scala_2_13",
          "@com_typesafe_scala_logging_scala_logging_2_13",
          "@com_yammer_metrics_metrics_core",
          "@commons_cli_commons_cli",
          "@io_dropwizard_metrics_metrics_core",
          "@net_sf_jopt_simple_jopt_simple",
          "@net_sourceforge_argparse4j_argparse4j",
          "@org_apache_kafka_kafka_group_coordinator",
          "@org_apache_kafka_kafka_metadata",
          "@org_apache_kafka_kafka_raft",
          "@org_apache_kafka_kafka_server_common",
          "@org_apache_kafka_kafka_storage",
          "@org_apache_kafka_kafka_storage_api",
          "@org_apache_kafka_kafka_tools_api",
          "@org_apache_zookeeper_zookeeper",
          "@org_bitbucket_b_c_jose4j",
          "@org_scala_lang_modules_scala_collection_compat_2_13",
          "@org_scala_lang_modules_scala_java8_compat_2_13",
          "@org_scala_lang_scala_reflect",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.slf4j:slf4j-log4j12
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_kafka_2_13_test",
      artifact = "org.apache.kafka:kafka_2.13:jar:test:7.5.0-154-ccs",
      artifact_sha256 = "b6655ef08707d9d4f970ceef4e01af16d2bba5b1ab6e24364d57f7d58944a30a",
      deps = [
          "@org_apache_kafka_kafka_clients",
          "@org_scala_lang_scala_library"
      ],
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_csv",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
          "@com_fasterxml_jackson_module_jackson_module_scala_2_13",
          "@com_typesafe_scala_logging_scala_logging_2_13",
          "@com_yammer_metrics_metrics_core",
          "@commons_cli_commons_cli",
          "@io_dropwizard_metrics_metrics_core",
          "@net_sf_jopt_simple_jopt_simple",
          "@net_sourceforge_argparse4j_argparse4j",
          "@org_apache_kafka_kafka_group_coordinator",
          "@org_apache_kafka_kafka_metadata",
          "@org_apache_kafka_kafka_raft",
          "@org_apache_kafka_kafka_server_common",
          "@org_apache_kafka_kafka_storage",
          "@org_apache_kafka_kafka_storage_api",
          "@org_apache_kafka_kafka_tools_api",
          "@org_apache_zookeeper_zookeeper",
          "@org_bitbucket_b_c_jose4j",
          "@org_scala_lang_modules_scala_collection_compat_2_13",
          "@org_scala_lang_modules_scala_java8_compat_2_13",
          "@org_scala_lang_scala_reflect",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.slf4j:slf4j-log4j12
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_kafka_clients",
      artifact = "org.apache.kafka:kafka-clients:7.5.0-154-ccs",
      artifact_sha256 = "75d111d03be1116cca8faddbd77e5ab40be440b90cdb5a816fd6d50831ffff24",
      runtime_deps = [
          "@com_github_luben_zstd_jni",
          "@org_lz4_lz4_java",
          "@org_slf4j_slf4j_api",
          "@org_xerial_snappy_snappy_java"
      ],
    # EXCLUDES org.slf4j:slf4j-log4j12
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_kafka_clients_test",
      artifact = "org.apache.kafka:kafka-clients:jar:test:7.5.0-154-ccs",
      artifact_sha256 = "c1d825f1db253786da44f09b2288b44a7ee3615886e3578231c09f961e979635",
      runtime_deps = [
          "@com_github_luben_zstd_jni",
          "@org_lz4_lz4_java",
          "@org_slf4j_slf4j_api",
          "@org_xerial_snappy_snappy_java"
      ],
    # EXCLUDES org.slf4j:slf4j-log4j12
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_kafka_group_coordinator",
      artifact = "org.apache.kafka:kafka-group-coordinator:7.5.0-154-ccs",
      artifact_sha256 = "e3b5f76e9e67133edb0f27cd9d3e472940f93dadba37fc6b0c0c359e33c18baf",
      runtime_deps = [
          "@org_apache_kafka_kafka_clients",
          "@org_apache_kafka_kafka_metadata",
          "@org_apache_kafka_kafka_server_common",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_log4j_appender",
      artifact = "org.apache.kafka:kafka-log4j-appender:7.5.0-154-ccs",
      artifact_sha256 = "38583098eefa0f43dad97f8962f71ad79abd26816d89be8a61e6393252b69c3a",
      runtime_deps = [
          "@org_apache_kafka_kafka_clients"
      ],
  )


  import_external(
      name = "org_apache_kafka_kafka_metadata",
      artifact = "org.apache.kafka:kafka-metadata:7.5.0-154-ccs",
      artifact_sha256 = "f09d56996da55fd0ea58be37d9d43f3acb6d7ca0350882709048d837b6d9a1d4",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
          "@com_yammer_metrics_metrics_core",
          "@org_apache_kafka_kafka_clients",
          "@org_apache_kafka_kafka_raft",
          "@org_apache_kafka_kafka_server_common"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_raft",
      artifact = "org.apache.kafka:kafka-raft:7.5.0-154-ccs",
      artifact_sha256 = "2ad8103520961d9d2cd6b4e471356187211afb17dda5abb3c5f537b638d31563",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@org_apache_kafka_kafka_clients",
          "@org_apache_kafka_kafka_server_common",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_server_common",
      artifact = "org.apache.kafka:kafka-server-common:7.5.0-154-ccs",
      artifact_sha256 = "517de97e9cf4b0d9b2e41d8956fe9c92b586e4d6eb3dd82660fd3979ad3f7564",
      runtime_deps = [
          "@com_yammer_metrics_metrics_core",
          "@net_sf_jopt_simple_jopt_simple",
          "@org_apache_kafka_kafka_clients",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_storage",
      artifact = "org.apache.kafka:kafka-storage:7.5.0-154-ccs",
      artifact_sha256 = "9fc66c2fe78c403520b40976b17b859b677565f607c3bb7b1539ca80b8ee9946",
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@org_apache_kafka_kafka_clients",
          "@org_apache_kafka_kafka_server_common",
          "@org_apache_kafka_kafka_storage_api",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_storage_api",
      artifact = "org.apache.kafka:kafka-storage-api:7.5.0-154-ccs",
      artifact_sha256 = "c9eb09e0c4cadfd214b271de5ca89a25d358110286d02fb6f385e134942c1f44",
      runtime_deps = [
          "@org_apache_kafka_kafka_clients",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_apache_kafka_kafka_streams",
      artifact = "org.apache.kafka:kafka-streams:7.5.0-154-ccs",
      artifact_sha256 = "c54722b64c3947bcb640d12ae1f5410a7fb6a6757c1a92f413a1ec7d7ec167cd",
      deps = [
          "@org_apache_kafka_kafka_clients",
          "@org_rocksdb_rocksdbjni"
      ],
      runtime_deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@org_slf4j_slf4j_api"
      ],
      neverlink = 1,
      generated_linkable_rule_name = "linkable",
  )


  import_external(
      name = "org_apache_kafka_kafka_tools",
      artifact = "org.apache.kafka:kafka-tools:7.5.0-154-ccs",
      artifact_sha256 = "fa5d5bac345c07e9c616cf214abcaf59ab9d43d2e23794e023f4edc86a39bab8",
      runtime_deps = [
          "@ch_qos_reload4j_reload4j",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
          "@com_fasterxml_jackson_jaxrs_jackson_jaxrs_json_provider",
          "@net_sf_jopt_simple_jopt_simple",
          "@net_sourceforge_argparse4j_argparse4j",
          "@org_apache_kafka_kafka_clients",
          "@org_apache_kafka_kafka_log4j_appender",
          "@org_apache_kafka_kafka_server_common",
          "@org_apache_kafka_kafka_tools_api",
          "@org_bitbucket_b_c_jose4j",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_kafka_kafka_tools_api",
      artifact = "org.apache.kafka:kafka-tools-api:7.5.0-154-ccs",
      artifact_sha256 = "2980708a83daf09330c2b666a09ffdd407c198be8f4a113d71b8e90f9b60e3de",
      runtime_deps = [
          "@org_apache_kafka_kafka_clients"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
