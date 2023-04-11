load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_validator_commons_validator",
      artifact = "commons-validator:commons-validator:1.7",
      artifact_sha256 = "4d74f4ce4fb68b2617edad086df6defdf9338467d2377d2c62e69038e1c4f02f",
      deps = [
          "@commons_collections_commons_collections",
          "@commons_digester_commons_digester",
          "@commons_logging_commons_logging"
      ],
    # EXCLUDES commons-beanutils:commons-beanutils
  )
