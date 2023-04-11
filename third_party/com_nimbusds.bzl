load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_nimbusds_content_type",
      artifact = "com.nimbusds:content-type:2.2",
      artifact_sha256 = "730f1816196145e88275093c147f2e6da3c3e541207acd3503a1b06129b9bea9",
  )


  import_external(
      name = "com_nimbusds_lang_tag",
      artifact = "com.nimbusds:lang-tag:1.6",
      artifact_sha256 = "409d056d9e5f566ac267f980e82027257dd864731abde7af8eba1f65213fbf4c",
  )


  import_external(
      name = "com_nimbusds_nimbus_jose_jwt",
      artifact = "com.nimbusds:nimbus-jose-jwt:9.22",
      artifact_sha256 = "f7c80eb70d5163e13452f76e85d4e4782e4ae2d8dc46fec3165d63a88f2c0799",
      deps = [
          "@com_github_stephenc_jcip_jcip_annotations"
      ],
  )


  import_external(
      name = "com_nimbusds_oauth2_oidc_sdk",
      artifact = "com.nimbusds:oauth2-oidc-sdk:9.35",
      artifact_sha256 = "95a5b7a5054fc281061b81d2354d0b7b66a862183e7d85d75a82c5ddb8867293",
      deps = [
          "@com_github_stephenc_jcip_jcip_annotations",
          "@com_nimbusds_content_type",
          "@com_nimbusds_lang_tag",
          "@com_nimbusds_nimbus_jose_jwt",
          "@net_minidev_json_smart"
      ],
  )
