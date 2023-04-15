load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_minidev_accessors_smart",
      artifact = "net.minidev:accessors-smart:2.4.8",
      artifact_sha256 = "7dd705aa1ac0e030f8ee2624e8e77239ae1eef6ccc2621c0b8c189866ee1c42c",
      deps = [
          "@org_ow2_asm_asm"
      ],
  )


  import_external(
      name = "net_minidev_json_smart",
      artifact = "net.minidev:json-smart:2.4.8",
      artifact_sha256 = "174a9ad578b56644e62b3965d8bf94ac3a76e707c6343b8abac9d3671438b4b2",
      deps = [
          "@net_minidev_accessors_smart"
      ],
  )
