load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_yammer_metrics_metrics_core",
      artifact = "com.yammer.metrics:metrics-core:2.2.0",
      artifact_sha256 = "6b7a14a6f34c10f8683f7b5e2f39df0f07b58c7dff0e468ebbc713905c46979c",
      runtime_deps = [
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
