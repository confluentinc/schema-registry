load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_typesafe_scala_logging_scala_logging_2_13",
      artifact = "com.typesafe.scala-logging:scala-logging_2.13:3.9.4",
      artifact_sha256 = "4366f59b97c6ffc617787d81e494b62d9aecf424dafb1ce9d39d855c5171b18a",
      runtime_deps = [
          "@org_scala_lang_scala_library",
          "@org_scala_lang_scala_reflect",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
