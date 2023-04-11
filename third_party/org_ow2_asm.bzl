load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_ow2_asm_asm",
      artifact = "org.ow2.asm:asm:9.3",
      artifact_sha256 = "1263369b59e29c943918de11d6d6152e2ec6085ce63e5710516f8c67d368e4bc",
  )


  import_external(
      name = "org_ow2_asm_asm_analysis",
      artifact = "org.ow2.asm:asm-analysis:9.3",
      artifact_sha256 = "37fd5392bb2cf4c15f202ffefd46d0e92bb34ff848c549f30d426a60d6b29495",
      deps = [
          "@org_ow2_asm_asm_tree"
      ],
  )


  import_external(
      name = "org_ow2_asm_asm_commons",
      artifact = "org.ow2.asm:asm-commons:9.3",
      artifact_sha256 = "a347c24732db2aead106b6e5996a015b06a3ef86e790a4f75b61761f0d2f7f39",
      deps = [
          "@org_ow2_asm_asm",
          "@org_ow2_asm_asm_analysis",
          "@org_ow2_asm_asm_tree"
      ],
  )


  import_external(
      name = "org_ow2_asm_asm_tree",
      artifact = "org.ow2.asm:asm-tree:9.3",
      artifact_sha256 = "ae629c2609f39681ef8d140a42a23800464a94f2d23e36d8f25cd10d5e4caff4",
      deps = [
          "@org_ow2_asm_asm"
      ],
  )
