load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_dropwizard_metrics_metrics_core",
      artifact = "io.dropwizard.metrics:metrics-core:4.1.12.1",
      artifact_sha256 = "cec34936faa625039f4e46123eccaa10a9eb9d9a40c6075830cd81330e259cf9",
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
