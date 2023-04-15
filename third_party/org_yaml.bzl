load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_yaml_snakeyaml",
      artifact = "org.yaml:snakeyaml:1.32",
      artifact_sha256 = "44d4f34d142076092c33f159a278ea60ea7474a03c3038138984b4fc0ecc15d3",
  )
