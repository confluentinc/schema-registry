load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_bitbucket_b_c_jose4j",
      artifact = "org.bitbucket.b_c:jose4j:0.7.9",
      artifact_sha256 = "ea504a7e42599b16fa78ee79bae909dd9441523ac0530d96a708683c0ebaaf02",
      runtime_deps = [
          "@org_slf4j_slf4j_api"
      ],
  )
