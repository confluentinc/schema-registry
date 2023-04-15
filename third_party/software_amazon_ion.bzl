load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "software_amazon_ion_ion_java",
      artifact = "software.amazon.ion:ion-java:1.0.2",
      artifact_sha256 = "0d127b205a1fce0abc2a3757a041748651bc66c15cf4c059bac5833b27d471a5",
  )
