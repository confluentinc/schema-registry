load("@bazel_tools//tools/build_defs/repo:jvm.bzl", "jvm_maven_import_external")

_default_server_urls = ["https://repo.maven.apache.org/maven2/",
                        "https://mvnrepository.com/artifact",
                        "https://maven-central.storage.googleapis.com",
                        "http://gitblit.github.io/gitblit-maven",
                        "https://repository.mulesoft.org/nexus/content/repositories/public/",]

def safe_exodus_maven_import_external(name, artifact, **kwargs):
  if native.existing_rule(name) == None:
        exodus_maven_import_external(
            name = name,
            artifact = artifact,
            **kwargs
        )


def exodus_maven_import_external(name, artifact, **kwargs):
  fetch_sources = kwargs.get("srcjar_sha256") != None
  exodus_maven_import_external_sources(name, artifact, fetch_sources, **kwargs)

def exodus_snapshot_maven_import_external(name, artifact, **kwargs):
  exodus_maven_import_external_sources(name, artifact, True, **kwargs)

def exodus_maven_import_external_sources(name, artifact, fetch_sources, **kwargs):
  jvm_maven_import_external(
      name = name,
      artifact = artifact,
      licenses = ["notice"],  # Apache 2.0
      fetch_sources = fetch_sources,
      server_urls = _default_server_urls,
      **kwargs
  )