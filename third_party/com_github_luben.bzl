load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_github_luben_zstd_jni",
      artifact = "com.github.luben:zstd-jni:1.5.2-1",
      artifact_sha256 = "93f7e4cbc907c2650f89f9f0bec94873735a58f1e4b66a54973294e4ec1878e8",
  )
