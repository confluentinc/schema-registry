load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_projectreactor_netty_reactor_netty_core",
      artifact = "io.projectreactor.netty:reactor-netty-core:1.0.23",
      deps = [
          "@io_netty_netty_handler",
          "@io_netty_netty_handler_proxy",
          "@io_netty_netty_resolver_dns",
          "@io_netty_netty_resolver_dns_native_macos_osx_x86_64",
          "@io_netty_netty_transport_native_epoll_linux_x86_64",
          "@io_projectreactor_reactor_core"
      ],
    # EXCLUDES commons-logging:commons-logging
  )


  import_external(
      name = "io_projectreactor_netty_reactor_netty_http",
      artifact = "io.projectreactor.netty:reactor-netty-http:1.0.23",
      deps = [
          "@io_netty_netty_codec_http",
          "@io_netty_netty_codec_http2",
          "@io_netty_netty_resolver_dns",
          "@io_netty_netty_resolver_dns_native_macos_osx_x86_64",
          "@io_netty_netty_transport_native_epoll_linux_x86_64",
          "@io_projectreactor_netty_reactor_netty_core",
          "@io_projectreactor_reactor_core"
      ],
  )
