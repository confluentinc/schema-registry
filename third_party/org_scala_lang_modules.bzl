load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_scala_lang_modules_scala_collection_compat_2_13",
      artifact = "org.scala-lang.modules:scala-collection-compat_2.13:2.6.0",
      artifact_sha256 = "7358248dc7c58b118e4d830f4128a6e72773cb0048587182c3db3414a4177b44",
      runtime_deps = [
          "@org_scala_lang_scala_library"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )


  import_external(
      name = "org_scala_lang_modules_scala_java8_compat_2_13",
      artifact = "org.scala-lang.modules:scala-java8-compat_2.13:1.0.2",
      artifact_sha256 = "90d5b13656be93fb779b8d7c723efa2498a34af06273bb5204afb65f85a20c1b",
      runtime_deps = [
          "@org_scala_lang_scala_library"
      ],
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
