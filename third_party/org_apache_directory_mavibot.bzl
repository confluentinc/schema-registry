load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_directory_mavibot_mavibot",
      artifact = "org.apache.directory.mavibot:mavibot:1.0.0-M8",
      artifact_sha256 = "76b991aa38dd491974b4fc2f5aef226d83e8ef85e0fab8fe71b067a97a8c753f",
      deps = [
          "@commons_collections_commons_collections",
          "@org_slf4j_slf4j_api"
      ],
  )
