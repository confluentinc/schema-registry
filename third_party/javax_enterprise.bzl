load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_enterprise_cdi_api",
      artifact = "javax.enterprise:cdi-api:1.0",
      artifact_sha256 = "1f10b2204cc77c919301f20ff90461c3df1b6e6cb148be1c2d22107f4851d423",
      deps = [
          "@javax_annotation_jsr250_api",
          "@javax_inject_javax_inject"
      ],
    # EXCLUDES javax.el:el-api
    # EXCLUDES org.jboss.ejb3:jboss-ejb3-api
    # EXCLUDES org.jboss.interceptor:jboss-interceptor-api
  )
