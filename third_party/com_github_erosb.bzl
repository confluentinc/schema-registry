load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_github_erosb_everit_json_schema",
      artifact = "com.github.erosb:everit-json-schema:1.14.1",
      artifact_sha256 = "1201bdf920b77d9a3238096258e763eb3266224b7012dfe8c4debc3dc2388145",
      deps = [
          "@com_damnhandy_handy_uri_templates",
          "@com_google_re2j_re2j",
          "@commons_validator_commons_validator",
          "@org_json_json"
      ],
  )
