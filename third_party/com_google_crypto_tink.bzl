load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_crypto_tink_tink",
      artifact = "com.google.crypto.tink:tink:1.8.0",
      artifact_sha256 = "92b8676c15b11a7faa15f3d1f05ed776a2897da3deba18c528ff32043339f248",
      deps = [
          "@androidx_annotation_annotation",
          "@com_google_code_findbugs_jsr305",
          "@com_google_code_gson_gson",
          "@com_google_errorprone_error_prone_annotations",
          "@com_google_http_client_google_http_client",
          "@com_google_protobuf_protobuf_java",
          "@joda_time_joda_time"
      ],
  )


  import_external(
      name = "com_google_crypto_tink_tink_awskms",
      artifact = "com.google.crypto.tink:tink-awskms:1.8.0",
      artifact_sha256 = "ebd71ea3913b36d1e97dac37c4022e139f1bc1448c0356f689dab79b665a740a",
      deps = [
          "@com_amazonaws_aws_java_sdk_core",
          "@com_amazonaws_aws_java_sdk_kms",
          "@com_google_auto_service_auto_service_annotations",
          "@com_google_code_findbugs_jsr305",
          "@com_google_crypto_tink_tink",
          "@com_google_errorprone_error_prone_annotations",
          "@com_google_guava_guava"
      ],
  )


  import_external(
      name = "com_google_crypto_tink_tink_gcpkms",
      artifact = "com.google.crypto.tink:tink-gcpkms:1.8.0",
      artifact_sha256 = "6ff41f00d04ba35abf41a0af375c31d8de4af5d1398d76be9c3933ab7a7411b2",
      deps = [
          "@com_google_api_client_google_api_client",
          "@com_google_apis_google_api_services_cloudkms",
          "@com_google_auth_google_auth_library_oauth2_http",
          "@com_google_auto_service_auto_service_annotations",
          "@com_google_code_findbugs_jsr305",
          "@com_google_crypto_tink_tink",
          "@com_google_errorprone_error_prone_annotations",
          "@com_google_http_client_google_http_client",
          "@com_google_http_client_google_http_client_gson",
          "@com_google_oauth_client_google_oauth_client"
      ],
  )
