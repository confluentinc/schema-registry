load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_azure_azure_core",
      artifact = "com.azure:azure-core:1.33.0",
      artifact_sha256 = "12644af67dc7449e1f3557561d87e910b677946008ca52d79cf546a352c46713",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_xml",
          "@com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
          "@io_projectreactor_reactor_core",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "com_azure_azure_core_http_netty",
      artifact = "com.azure:azure-core-http-netty:1.12.6",
      artifact_sha256 = "446daba91c59f3e2e50623632294eb4ac48fc5e337a23a470311a951563239b7",
      deps = [
          "@com_azure_azure_core",
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec_http",
          "@io_netty_netty_codec_http2",
          "@io_netty_netty_handler",
          "@io_netty_netty_handler_proxy",
          "@io_netty_netty_tcnative_boringssl_static",
          "@io_netty_netty_transport_native_epoll_linux_x86_64",
          "@io_netty_netty_transport_native_kqueue_osx_x86_64",
          "@io_netty_netty_transport_native_unix_common",
          "@io_projectreactor_netty_reactor_netty_http"
      ],
  )


  import_external(
      name = "com_azure_azure_identity",
      artifact = "com.azure:azure-identity:1.7.3",
      artifact_sha256 = "fc37b8c2de1f47479359799d30b08b0f2bd8119e3db326cb3d357ab104a8cf8a",
      deps = [
          "@com_azure_azure_core",
          "@com_azure_azure_core_http_netty",
          "@com_microsoft_azure_msal4j",
          "@com_microsoft_azure_msal4j_persistence_extension",
          "@net_java_dev_jna_jna_platform"
      ],
  )


  import_external(
      name = "com_azure_azure_security_keyvault_keys",
      artifact = "com.azure:azure-security-keyvault-keys:4.5.1",
      artifact_sha256 = "a2d3c4bcc7bc199cbda089ad813b59ba7e29f4b553e050bea26c3004fa72c1b8",
      deps = [
          "@com_azure_azure_core",
          "@com_azure_azure_core_http_netty"
      ],
  )
