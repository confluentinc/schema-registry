load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_sf_ehcache_ehcache",
      artifact = "net.sf.ehcache:ehcache:2.8.5",
      artifact_sha256 = "1160a0a0c0562dcb83afb5cdb8c11deb75778614110a0e6f18e38e89e54ee041",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )
