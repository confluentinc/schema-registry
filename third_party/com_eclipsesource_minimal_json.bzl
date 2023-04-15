load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_eclipsesource_minimal_json_minimal_json",
      artifact = "com.eclipsesource.minimal-json:minimal-json:0.9.5",
      artifact_sha256 = "69ff84463d1d7cc5cfdb8baf2b6e5f7af153d351bf0dbee3bbc8916247afac74",
  )
