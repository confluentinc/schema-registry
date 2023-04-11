load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_logging_commons_logging",
      artifact = "commons-logging:commons-logging:1.2",
      artifact_sha256 = "daddea1ea0be0f56978ab3006b8ac92834afeefbd9b7e4e6316fca57df0fa636",
  )
