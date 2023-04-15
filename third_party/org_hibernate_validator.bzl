load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_hibernate_validator_hibernate_validator",
      artifact = "org.hibernate.validator:hibernate-validator:6.1.7.Final",
      artifact_sha256 = "77a7caa6530a152db8a65be7b7ef2008cf6ca2947453cfea792f37855f3d1a0f",
      deps = [
          "@com_fasterxml_classmate",
          "@jakarta_validation_jakarta_validation_api",
          "@org_jboss_logging_jboss_logging"
      ],
  )
