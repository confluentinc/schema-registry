load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_powermock_powermock_core",
      artifact = "org.powermock:powermock-core:2.0.9",
      artifact_sha256 = "e5183d1e197bcd67e8f86eeb5acc4cc4b4a7aa993e9daa249f8d8d6973f06c49",
      deps = [
          "@net_bytebuddy_byte_buddy",
          "@net_bytebuddy_byte_buddy_agent",
          "@org_javassist_javassist",
          "@org_powermock_powermock_reflect"
      ],
  )


  import_external(
      name = "org_powermock_powermock_module_junit4",
      artifact = "org.powermock:powermock-module-junit4:2.0.9",
      artifact_sha256 = "d0e8a83183a9a8a18ff83e1592a611fa206cab0838466ce367e3d0a851a274e2",
      deps = [
          "@junit_junit",
          "@org_hamcrest_hamcrest_core",
          "@org_powermock_powermock_module_junit4_common"
      ],
  )


  import_external(
      name = "org_powermock_powermock_module_junit4_common",
      artifact = "org.powermock:powermock-module-junit4-common:2.0.9",
      artifact_sha256 = "446f975ffa98960ab6eafccb5c4d1e2cb5747f7d80cda653548a02d584289e83",
      deps = [
          "@junit_junit",
          "@org_hamcrest_hamcrest_core",
          "@org_powermock_powermock_core",
          "@org_powermock_powermock_reflect"
      ],
  )


  import_external(
      name = "org_powermock_powermock_reflect",
      artifact = "org.powermock:powermock-reflect:2.0.9",
      artifact_sha256 = "a1374bd368b52b54b252d5281b9391363b58cb667a6375242fd6a3f482bc8c23",
      deps = [
          "@net_bytebuddy_byte_buddy",
          "@net_bytebuddy_byte_buddy_agent",
          "@org_objenesis_objenesis"
      ],
  )
