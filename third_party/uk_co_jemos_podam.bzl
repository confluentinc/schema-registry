load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "uk_co_jemos_podam_podam",
      artifact = "uk.co.jemos.podam:podam:7.2.11.RELEASE",
      artifact_sha256 = "40a983c5ab516bb73bdb930ed7a563fdfc53c62b66df0f8a3670bcf761c676c8",
      deps = [
          "@javax_annotation_javax_annotation_api",
          "@javax_validation_validation_api",
          "@net_jcip_jcip_annotations",
          "@org_apache_commons_commons_lang3",
          "@org_slf4j_slf4j_api"
      ],
  )
