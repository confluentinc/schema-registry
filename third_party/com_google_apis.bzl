load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_apis_google_api_services_cloudkms",
      artifact = "com.google.apis:google-api-services-cloudkms:v1-rev20221107-2.0.0",
      artifact_sha256 = "6c98eb64a9b692127bc9f144837a6f70d5c391132ac198ebb92bfb326d3d2ab4",
      deps = [
          "@com_google_api_client_google_api_client"
      ],
  )
