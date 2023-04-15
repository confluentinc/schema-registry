load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_rocksdb_rocksdbjni",
      artifact = "org.rocksdb:rocksdbjni:7.1.2",
      artifact_sha256 = "6d3b31904f170efc2171524462ce7290865c1e1efb47760469303d5b16a4b767",
  )
