load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_httpcomponents_httpclient",
      artifact = "org.apache.httpcomponents:httpclient:4.5.13",
      artifact_sha256 = "6fe9026a566c6a5001608cf3fc32196641f6c1e5e1986d1037ccdbd5f31ef743",
      deps = [
          "@commons_codec_commons_codec",
          "@commons_logging_commons_logging",
          "@org_apache_httpcomponents_httpcore"
      ],
  )


  import_external(
      name = "org_apache_httpcomponents_httpcore",
      artifact = "org.apache.httpcomponents:httpcore:4.4.15",
      artifact_sha256 = "3cbaed088c499a10f96dde58f39dc0e7985171abd88138ca1655a872011bb142",
  )
