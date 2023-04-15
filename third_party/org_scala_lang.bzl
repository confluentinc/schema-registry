load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_scala_lang_scala_library",
      artifact = "org.scala-lang:scala-library:2.13.10",
      artifact_sha256 = "e6ca607c3fce03e8fa38af3374ce1f8bb098e316e8bf6f6d27331360feddb1c1",
  )


  import_external(
      name = "org_scala_lang_scala_reflect",
      artifact = "org.scala-lang:scala-reflect:2.13.10",
      artifact_sha256 = "62bd7a7198193c5373a992c122990279e413af3307162472a5d3cbb8ecadb35e",
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
