load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_maven_plugin_tools_maven_plugin_annotations",
      artifact = "org.apache.maven.plugin-tools:maven-plugin-annotations:3.6.0",
      artifact_sha256 = "9e2434820dd2ba44ad70a66e5b2a9993a2a8b047ceabc3e850e4858cbf3f91c3",
      deps = [
          "@org_apache_maven_maven_artifact"
      ],
  )
