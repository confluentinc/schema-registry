load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_oauth_client_google_oauth_client",
      artifact = "com.google.oauth-client:google-oauth-client:1.34.1",
      artifact_sha256 = "193edf97aefa28b93c5892bdc598bac34fa4c396588030084f290b1440e8b98a",
      deps = [
          "@com_google_guava_guava",
          "@com_google_http_client_google_http_client",
          "@com_google_http_client_google_http_client_gson"
      ],
  )
