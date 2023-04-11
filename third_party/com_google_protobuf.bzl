load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_protobuf_protobuf_java",
      artifact = "com.google.protobuf:protobuf-java:3.19.6",
      artifact_sha256 = "6a9a2dff91dcf71f85be71ae971f6164b5a631dcd34bff08f0618535ca44ad02",
  )


  import_external(
      name = "com_google_protobuf_protobuf_java_util",
      artifact = "com.google.protobuf:protobuf-java-util:3.19.6",
      artifact_sha256 = "8c536b2ca85f4e0891b85060b9f697e9e61c074c3bcc887916904562d2ef8108",
      deps = [
          "@com_google_code_findbugs_jsr305",
          "@com_google_code_gson_gson",
          "@com_google_errorprone_error_prone_annotations",
          "@com_google_guava_guava",
          "@com_google_j2objc_j2objc_annotations",
          "@com_google_protobuf_protobuf_java"
      ],
  )
