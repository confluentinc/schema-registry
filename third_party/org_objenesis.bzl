load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_objenesis_objenesis",
      artifact = "org.objenesis:objenesis:3.2",
      artifact_sha256 = "03d960bd5aef03c653eb000413ada15eb77cdd2b8e4448886edf5692805e35f3",
  )
