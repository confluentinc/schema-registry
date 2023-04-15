load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_zookeeper_zookeeper",
      artifact = "org.apache.zookeeper:zookeeper:3.6.3",
      artifact_sha256 = "ac44de96d37a80d463c1e01aaa2ea86b19490fa17a37438d14491456c0e4a911",
      deps = [
          "@io_netty_netty_handler",
          "@io_netty_netty_transport_native_epoll",
          "@log4j_log4j",
          "@org_apache_yetus_audience_annotations",
          "@org_apache_zookeeper_zookeeper_jute",
          "@org_slf4j_slf4j_api",
          "@org_slf4j_slf4j_log4j12"
      ],
    # EXCLUDES io.netty:netty-codec
  )


  import_external(
      name = "org_apache_zookeeper_zookeeper_jute",
      artifact = "org.apache.zookeeper:zookeeper-jute:3.6.3",
      artifact_sha256 = "6f463ade2b777f81e6c3e145b3fc03d4b39131b73dcf8c7f4b89ac4b1bacc9c5",
      deps = [
          "@org_apache_yetus_audience_annotations"
      ],
  )
