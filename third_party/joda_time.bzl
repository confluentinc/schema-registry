load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "joda_time_joda_time",
      artifact = "joda-time:joda-time:2.10.3",
      artifact_sha256 = "ebb6a6aade36fba2e5aa3f2b98ff9904f20f6f59db1ec6513be5e97d0c578e89",
  )
