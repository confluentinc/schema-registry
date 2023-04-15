load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_jackson_datatype_jackson_datatype_guava",
      artifact = "com.fasterxml.jackson.datatype:jackson-datatype-guava:2.14.2",
      artifact_sha256 = "07cbb8b8a354dfc067fedf66e19226a7a8a6f56e46d2b78b85cbac5149aba71d",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_google_guava_guava"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_datatype_jackson_datatype_jdk8",
      artifact = "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.14.2",
      artifact_sha256 = "aa55d3d545e02d959a6e8c83927acc55c9937d717d4d3fd962d27ec7b431b8c4",
      deps = [
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_datatype_jackson_datatype_joda",
      artifact = "com.fasterxml.jackson.datatype:jackson-datatype-joda:2.14.2",
      artifact_sha256 = "ab3433a5f984544f48e938600ae9fa65f29ee1a8c50618938cd172da58f89507",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind",
          "@joda_time_joda_time"
      ],
  )


  import_external(
      name = "com_fasterxml_jackson_datatype_jackson_datatype_jsr310",
      artifact = "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.14.2",
      artifact_sha256 = "75651b65733ed94e4e28e4ba0817218d93e71e8a7f06f6ab3662752974d2bcae",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core",
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )
