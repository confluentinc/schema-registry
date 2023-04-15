load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_json_json",
      artifact = "org.json:json:20220320",
      artifact_sha256 = "1edf7fcea79a16b8dfdd3bc988ddec7f8908b1f7762fdf00d39acb037542747a",
  )
