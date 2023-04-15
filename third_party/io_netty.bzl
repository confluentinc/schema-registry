load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_netty_netty_buffer",
      artifact = "io.netty:netty-buffer:4.1.86.Final",
      artifact_sha256 = "e42e15f47c865266b1faa6e038ebfd7ddadcf9f4ae9e6617edd4881dbd4abe88",
      deps = [
          "@io_netty_netty_common"
      ],
  )


  import_external(
      name = "io_netty_netty_codec",
      artifact = "io.netty:netty-codec:4.1.86.Final",
      artifact_sha256 = "0456840b5c851dad6cab881cd1a9ad5d916db65d81048145df1d9a6d03325bea",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_codec_dns",
      artifact = "io.netty:netty-codec-dns:4.1.81.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_common",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_codec_http",
      artifact = "io.netty:netty-codec-http:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_common",
          "@io_netty_netty_handler",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_codec_http2",
      artifact = "io.netty:netty-codec-http2:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_codec_http",
          "@io_netty_netty_common",
          "@io_netty_netty_handler",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_codec_socks",
      artifact = "io.netty:netty-codec-socks:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_common",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_common",
      artifact = "io.netty:netty-common:4.1.86.Final",
      artifact_sha256 = "a35a3f16e7cd45c5d8529aa3e7702d4ef3b36213ea332db59744ea348fc2ae99",
  )


  import_external(
      name = "io_netty_netty_handler",
      artifact = "io.netty:netty-handler:4.1.63.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_resolver",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_handler_proxy",
      artifact = "io.netty:netty-handler-proxy:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_codec_http",
          "@io_netty_netty_codec_socks",
          "@io_netty_netty_common",
          "@io_netty_netty_transport"
      ],
  )


  import_external(
      name = "io_netty_netty_resolver",
      artifact = "io.netty:netty-resolver:4.1.86.Final",
      artifact_sha256 = "7628a1309d7f2443dc41d8923a7f269e2981b9616f80a999eb7264ae6bcbfdba",
      deps = [
          "@io_netty_netty_common"
      ],
  )


  import_external(
      name = "io_netty_netty_resolver_dns",
      artifact = "io.netty:netty-resolver-dns:4.1.81.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_codec",
          "@io_netty_netty_codec_dns",
          "@io_netty_netty_common",
          "@io_netty_netty_handler",
          "@io_netty_netty_resolver",
          "@io_netty_netty_transport"
      ],
    # EXCLUDES commons-logging:commons-logging
  )


  import_external(
      name = "io_netty_netty_resolver_dns_classes_macos",
      artifact = "io.netty:netty-resolver-dns-classes-macos:4.1.81.Final",
      deps = [
          "@io_netty_netty_common",
          "@io_netty_netty_resolver_dns",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_resolver_dns_native_macos_osx_x86_64",
      artifact = "io.netty:netty-resolver-dns-native-macos:jar:osx-x86_64:4.1.81.Final",
      deps = [
          "@io_netty_netty_resolver_dns_classes_macos"
      ],
    # EXCLUDES commons-logging:commons-logging
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static",
      artifact = "io.netty:netty-tcnative-boringssl-static:2.0.54.Final",
      artifact_sha256 = "1b9aeee2a775314eb972f97e30e93dcc1472c09f0e4a43c3fe7afa922e669dea",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_linux_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_windows_x86_64",
          "@io_netty_netty_tcnative_classes"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
      artifact = "io.netty:netty-tcnative-boringssl-static:jar:linux-aarch_64:2.0.54.Final",
      artifact_sha256 = "4cc801f39a8fa71769fd4913fb98890e27a3fe16f3f9457ebba45cc30bdaed5e",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_windows_x86_64"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static_linux_x86_64",
      artifact = "io.netty:netty-tcnative-boringssl-static:jar:linux-x86_64:2.0.54.Final",
      artifact_sha256 = "41f50bb80e9b6d6716f1ac95087384d2ae1f2062959651a2178c007e513ed541",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_windows_x86_64"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
      artifact = "io.netty:netty-tcnative-boringssl-static:jar:osx-aarch_64:2.0.54.Final",
      artifact_sha256 = "28d5554ef2d1a680e5f871704c7f97cddb6b391b6ed17659ef0d6d6e01df7869",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_linux_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_windows_x86_64"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static_osx_x86_64",
      artifact = "io.netty:netty-tcnative-boringssl-static:jar:osx-x86_64:2.0.54.Final",
      artifact_sha256 = "49b6cdca6dade47ab29ce5317baf32c2671f56304b6e40074f7950718e4275fd",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_linux_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_windows_x86_64"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_boringssl_static_windows_x86_64",
      artifact = "io.netty:netty-tcnative-boringssl-static:jar:windows-x86_64:2.0.54.Final",
      artifact_sha256 = "7af3a38de275c58281e1f18593a523331db69890baabf5c5734d805dd1dddb71",
      deps = [
          "@io_netty_netty_tcnative_boringssl_static_linux_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_linux_x86_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_aarch_64",
          "@io_netty_netty_tcnative_boringssl_static_osx_x86_64"
      ],
  )


  import_external(
      name = "io_netty_netty_tcnative_classes",
      artifact = "io.netty:netty-tcnative-classes:2.0.54.Final",
      artifact_sha256 = "98977e279a66778e816f0ed25ea5b7d20b1cffc6469de73e5e3e9accda2aceb4",
    # EXCLUDES io.netty:netty-jni-util
  )


  import_external(
      name = "io_netty_netty_transport",
      artifact = "io.netty:netty-transport:4.1.86.Final",
      artifact_sha256 = "f6726dcd54e4922b46b3b4f4467b443a70a30eb08a62620c8fe502d8cb802c9f",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_resolver"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_classes_epoll",
      artifact = "io.netty:netty-transport-classes-epoll:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_classes_kqueue",
      artifact = "io.netty:netty-transport-classes-kqueue:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_native_epoll",
      artifact = "io.netty:netty-transport-native-epoll:4.1.63.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_native_epoll_linux_x86_64",
      artifact = "io.netty:netty-transport-native-epoll:jar:linux-x86_64:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport",
          "@io_netty_netty_transport_classes_epoll",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_native_kqueue_osx_x86_64",
      artifact = "io.netty:netty-transport-native-kqueue:jar:osx-x86_64:4.1.82.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport",
          "@io_netty_netty_transport_classes_kqueue",
          "@io_netty_netty_transport_native_unix_common"
      ],
  )


  import_external(
      name = "io_netty_netty_transport_native_unix_common",
      artifact = "io.netty:netty-transport-native-unix-common:4.1.63.Final",
      deps = [
          "@io_netty_netty_buffer",
          "@io_netty_netty_common",
          "@io_netty_netty_transport"
      ],
  )
