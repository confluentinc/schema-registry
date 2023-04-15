load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_eclipse_jetty_http2_http2_common",
      artifact = "org.eclipse.jetty.http2:http2-common:9.4.48.v20220622",
      artifact_sha256 = "4747a5d052ebebdd0002d4817926d020acee256fe57acca297b473b7bb349d9f",
      deps = [
          "@org_eclipse_jetty_http2_http2_hpack"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_http2_http2_hpack",
      artifact = "org.eclipse.jetty.http2:http2-hpack:9.4.48.v20220622",
      artifact_sha256 = "8c37d3074aee3ffe5fbcab4dfee1dc693e9349798767899ea4b7d79831139dab",
      deps = [
          "@org_eclipse_jetty_jetty_http",
          "@org_eclipse_jetty_jetty_io",
          "@org_eclipse_jetty_jetty_util"
      ],
  )


  import_external(
      name = "org_eclipse_jetty_http2_http2_server",
      artifact = "org.eclipse.jetty.http2:http2-server:9.4.48.v20220622",
      artifact_sha256 = "2b2b0c18f15a92baaa0dc6d28d0e417ca3e5f5d7146dee048c9be1a9d1440e5e",
      deps = [
          "@org_eclipse_jetty_http2_http2_common",
          "@org_eclipse_jetty_jetty_server"
      ],
  )
