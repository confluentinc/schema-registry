load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_damnhandy_handy_uri_templates",
      artifact = "com.damnhandy:handy-uri-templates:2.1.8",
      artifact_sha256 = "6b83846f2ff61d0aaa66997b64b883ec7b65cf13b50a4d7f58250996d429be2e",
      deps = [
          "@joda_time_joda_time"
      ],
  )
