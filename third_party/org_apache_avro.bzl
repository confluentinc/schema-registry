load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_avro_avro",
      artifact = "org.apache.avro:avro:1.11.0",
      artifact_sha256 = "b3e42815a3dddb0d9432035aa133122c3dffc03affea49493d46fcc2e7581a74",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@org_apache_commons_commons_compress",
          "@org_slf4j_slf4j_api"
      ],
  )
