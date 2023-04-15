load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_kcache_kcache",
      artifact = "io.kcache:kcache:4.0.11",
      artifact_sha256 = "eb94aa3016dfef392b1e65dcd80e6b1b8b6b489c54426ea853cc37edb4b77dbc",
      deps = [
          "@com_google_guava_guava",
          "@org_apache_kafka_kafka_clients",
          "@org_slf4j_slf4j_api"
      ],
  )
