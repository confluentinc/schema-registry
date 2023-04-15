
workspace(name = "lyao_77_schema_registry")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl","git_repository")

git_repository(
    name = "rules_jvm_test_discovery",
    remote = "git@github.com:wix-incubator/rules_jvm_test_discovery.git",
    commit = "4c1adcca5f0347704ddb6b16a7c7ad6e0e19ae29"
)

load("@rules_jvm_test_discovery//:junit.bzl", "junit_repositories")
junit_repositories()
      

load("//:third_party.bzl", "third_party_dependencies")

third_party_dependencies()

load(":junit5.bzl", "junit_jupiter_java_repositories", "junit_platform_java_repositories")

JUNIT_JUPITER_VERSION = "5.4.2"

JUNIT_PLATFORM_VERSION = "1.4.2"

junit_jupiter_java_repositories(
    version = JUNIT_JUPITER_VERSION,
)

junit_platform_java_repositories(
    version = JUNIT_PLATFORM_VERSION,
)
      



