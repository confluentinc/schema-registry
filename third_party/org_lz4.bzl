load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_lz4_lz4_java",
      artifact = "org.lz4:lz4-java:1.8.0",
      artifact_sha256 = "d74a3334fb35195009b338a951f918203d6bbca3d1d359033dc33edd1cadc9ef",
  )
