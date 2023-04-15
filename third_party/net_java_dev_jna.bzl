load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_java_dev_jna_jna",
      artifact = "net.java.dev.jna:jna:5.5.0",
      artifact_sha256 = "b308faebfe4ed409de8410e0a632d164b2126b035f6eacff968d3908cafb4d9e",
  )


  import_external(
      name = "net_java_dev_jna_jna_platform",
      artifact = "net.java.dev.jna:jna-platform:5.6.0",
      artifact_sha256 = "9ecea8bf2b1b39963939d18b70464eef60c508fed8820f9dcaba0c35518eabf7",
      deps = [
          "@net_java_dev_jna_jna"
      ],
  )
