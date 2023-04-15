load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_cli_commons_cli",
      artifact = "commons-cli:commons-cli:1.4",
      artifact_sha256 = "fd3c7c9545a9cdb2051d1f9155c4f76b1e4ac5a57304404a6eedb578ffba7328",
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
