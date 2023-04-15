load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_http_client_google_http_client",
      artifact = "com.google.http-client:google-http-client:1.42.0",
      artifact_sha256 = "82ca0e08171846d1768d5ac3f13244d6fe5a54102c14735ef40bf15d57d478e5",
      deps = [
          "@com_google_code_findbugs_jsr305",
          "@com_google_guava_guava",
          "@com_google_j2objc_j2objc_annotations",
          "@io_opencensus_opencensus_api",
          "@io_opencensus_opencensus_contrib_http_util",
          "@org_apache_httpcomponents_httpclient",
          "@org_apache_httpcomponents_httpcore"
      ],
  )


  import_external(
      name = "com_google_http_client_google_http_client_apache_v2",
      artifact = "com.google.http-client:google-http-client-apache-v2:1.42.0",
      artifact_sha256 = "1fc4964236b67cf3c5651d7ac1dff668f73b7810c7f1dc0862a0e5bc01608785",
      deps = [
          "@com_google_http_client_google_http_client",
          "@org_apache_httpcomponents_httpclient",
          "@org_apache_httpcomponents_httpcore"
      ],
  )


  import_external(
      name = "com_google_http_client_google_http_client_gson",
      artifact = "com.google.http-client:google-http-client-gson:1.42.0",
      artifact_sha256 = "cb852272c1cb0c8449d8b1a70f3e0f2c1efb2063e543183faa43078fb446f540",
      deps = [
          "@com_google_code_gson_gson",
          "@com_google_http_client_google_http_client"
      ],
  )
