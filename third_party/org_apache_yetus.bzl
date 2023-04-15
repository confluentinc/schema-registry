load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_yetus_audience_annotations",
      artifact = "org.apache.yetus:audience-annotations:0.5.0",
      artifact_sha256 = "c82631f06c75d46bf6524d95f0d6c2e3aef1b3eb4a7b584ca296624ef0d474be",
  )
