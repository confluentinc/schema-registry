load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_auth_google_auth_library_credentials",
      artifact = "com.google.auth:google-auth-library-credentials:1.5.3",
      artifact_sha256 = "7f6eaddde4fc20129472c76ba072b19c5a14cccd3a5784795975478c5d92a12c",
  )


  import_external(
      name = "com_google_auth_google_auth_library_oauth2_http",
      artifact = "com.google.auth:google-auth-library-oauth2-http:1.5.3",
      artifact_sha256 = "45073775620656b6c4ac5b4486928c3787d9f8de93fd3b446c2c377cc53a70e7",
      deps = [
          "@com_google_auth_google_auth_library_credentials",
          "@com_google_auto_value_auto_value_annotations",
          "@com_google_code_findbugs_jsr305",
          "@com_google_guava_guava",
          "@com_google_http_client_google_http_client",
          "@com_google_http_client_google_http_client_gson"
      ],
  )
