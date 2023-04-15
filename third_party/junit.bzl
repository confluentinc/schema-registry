load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "junit_junit",
      artifact = "junit:junit:4.13.2",
      artifact_sha256 = "8e495b634469d64fb8acfa3495a065cbacc8a0fff55ce1e31007be4c16dc57d3",
      deps = [
          "@org_hamcrest_hamcrest_core"
      ],
  )
