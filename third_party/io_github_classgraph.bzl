load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_github_classgraph_classgraph",
      artifact = "io.github.classgraph:classgraph:4.8.21",
      artifact_sha256 = "4cdfbb73c4c3e79b6707ec4f7520adb5d23c27baecdc39cfb145ff529ce5c151",
  )
