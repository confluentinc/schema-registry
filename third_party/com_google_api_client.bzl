load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_api_client_google_api_client",
      artifact = "com.google.api-client:google-api-client:1.35.2",
      artifact_sha256 = "f195cd6228d3f99fa7e30ff2dee60ad0f2c7923be31399a7dcdc1abd679aa22e",
      deps = [
          "@com_google_guava_guava",
          "@com_google_http_client_google_http_client",
          "@com_google_http_client_google_http_client_apache_v2",
          "@com_google_http_client_google_http_client_gson",
          "@com_google_oauth_client_google_oauth_client",
          "@org_apache_httpcomponents_httpclient",
          "@org_apache_httpcomponents_httpcore"
      ],
  )
