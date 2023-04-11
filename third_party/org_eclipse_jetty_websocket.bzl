load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_eclipse_jetty_websocket_javax_websocket_client_impl",
      artifact = "org.eclipse.jetty.websocket:javax-websocket-client-impl:9.4.48.v20220622",
      artifact_sha256 = "80a485804268036d619e02a99d913655bc5d350da0c4ee52fd26f294d853bfde",
      deps = [
          "@javax_websocket_javax_websocket_client_api",
          "@org_eclipse_jetty_websocket_websocket_client"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_websocket_javax_websocket_server_impl",
      artifact = "org.eclipse.jetty.websocket:javax-websocket-server-impl:9.4.48.v20220622",
      artifact_sha256 = "bba72545036038867437b6aaa61136329a4036ff6b4078b70b3b7654c7bea2d1",
      deps = [
          "@javax_websocket_javax_websocket_api",
          "@org_eclipse_jetty_jetty_annotations",
          "@org_eclipse_jetty_websocket_javax_websocket_client_impl",
          "@org_eclipse_jetty_websocket_websocket_server"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_websocket_websocket_api",
      artifact = "org.eclipse.jetty.websocket:websocket-api:9.4.48.v20220622",
      artifact_sha256 = "87fb052324d6c5e22f58fb729169913bb318d97921115640c3dca453c7eb19e1",
  )


  import_external(
      name = "org_eclipse_jetty_websocket_websocket_client",
      artifact = "org.eclipse.jetty.websocket:websocket-client:9.4.48.v20220622",
      artifact_sha256 = "432d9d85734be8acbdbc3656200d2b1d540574c9bebe0b4f75a3f8bbe402a2f1",
      deps = [
          "@org_eclipse_jetty_jetty_client",
          "@org_eclipse_jetty_jetty_io",
          "@org_eclipse_jetty_jetty_util",
          "@org_eclipse_jetty_websocket_websocket_common"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_websocket_websocket_common",
      artifact = "org.eclipse.jetty.websocket:websocket-common:9.4.48.v20220622",
      artifact_sha256 = "1f630339e7e7f6de5d7f47f9496495d7689ad41fc11565aaf11ce9e22ff6ccda",
      deps = [
          "@org_eclipse_jetty_jetty_io",
          "@org_eclipse_jetty_jetty_util",
          "@org_eclipse_jetty_websocket_websocket_api"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_websocket_websocket_server",
      artifact = "org.eclipse.jetty.websocket:websocket-server:9.4.48.v20220622",
      artifact_sha256 = "32ad18b3c610a5036c33d2e6bdf76eb46759fb8e067483d7a96f94e3909a4285",
      deps = [
          "@org_eclipse_jetty_jetty_http",
          "@org_eclipse_jetty_jetty_servlet",
          "@org_eclipse_jetty_websocket_websocket_client",
          "@org_eclipse_jetty_websocket_websocket_common",
          "@org_eclipse_jetty_websocket_websocket_servlet"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_websocket_websocket_servlet",
      artifact = "org.eclipse.jetty.websocket:websocket-servlet:9.4.48.v20220622",
      artifact_sha256 = "28aeea9ac33a3f6d22e31ed2a6079abc35ce3ec1d5c80e1cc13859f536680b25",
      deps = [
          "@javax_servlet_javax_servlet_api//:linkable",
          "@org_eclipse_jetty_websocket_websocket_api"
      ],
  )
