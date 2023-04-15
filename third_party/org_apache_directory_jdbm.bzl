load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_directory_jdbm_apacheds_jdbm1",
      artifact = "org.apache.directory.jdbm:apacheds-jdbm1:2.0.0-M3",
      artifact_sha256 = "9eeab7949fb6a1589164aee185bdbf89156fa20c3fd5d155b424069485d0ff78",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )
