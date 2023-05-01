load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

http_archive(
    name = "contrib_rules_jvm",
    sha256 = "09c022847c96f24d085e2c82a6174f0ab98218e6e0903d0793d69af9f771a291",
    strip_prefix = "rules_jvm-0.12.0",
    url = "https://github.com/bazel-contrib/rules_jvm/releases/download/v0.12.0/rules_jvm-v0.12.0.tar.gz",
)

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "56d8c5a5c91e1af73eca71a6fab2ced959b67c86d12ba37feedb0a2dfea441a6",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.37.0/rules_go-v0.37.0.zip",
    ],
)

http_archive(
    name = "rules_proto",
    sha256 = "dc3fb206a2cb3441b485eb1e423165b231235a1ea9b031b4433cf7bc1fa460dd",
    strip_prefix = "rules_proto-5.3.0-21.7",
    urls = [
        "https://github.com/bazelbuild/rules_proto/archive/refs/tags/5.3.0-21.7.tar.gz",
    ],
)
load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")
rules_proto_dependencies()
rules_proto_toolchains()

http_archive(
    name = "bazel_gazelle",
    sha256 = "ecba0f04f96b4960a5b250c8e8eeec42281035970aa8852dda73098274d14a1d",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
        "https://github.com/bazelbuild/bazel-gazelle/releases/download/v0.29.0/bazel-gazelle-v0.29.0.tar.gz",
    ],
)

http_archive(
    name = "bazel_skylib",
    sha256 = "b8a1527901774180afc798aeb28c4634bdccf19c4d98e7bdd1ce79d1fe9aaad7",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.4.1/bazel-skylib-1.4.1.tar.gz",
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies", "go_repository")

go_rules_dependencies()

go_register_toolchains(version = "1.19.5")

gazelle_dependencies()

go_repository(
    name = "org_golang_google_grpc",
    build_file_proto_mode = "disable",
    importpath = "google.golang.org/grpc",
    sum = "h1:SfXqXS5hkufcdZ/mHtYCh53P2b+92WQq/DZcKLgsFRs=",
    version = "v1.31.1",
)

#############################################################
# Define your own dependencies here using go_repository.
# Else, dependencies declared by rules_go/gazelle will be used
# The first declaration of an external repository "wins".
############################################################
load("@contrib_rules_jvm//:repositories.bzl", "contrib_rules_jvm_deps", "contrib_rules_jvm_gazelle_deps")

contrib_rules_jvm_deps()

contrib_rules_jvm_gazelle_deps()

# for linters config
load("@apple_rules_lint//lint:setup.bzl", "lint_setup")
lint_setup({
  "java-checkstyle": "//checkstyle:checkstyle_config",
  "java-spotbugs": "//findbugs:spotbugs_config",
})

load("@contrib_rules_jvm//:setup.bzl", "contrib_rules_jvm_setup")

contrib_rules_jvm_setup()

load("@contrib_rules_jvm//:gazelle_setup.bzl", "contrib_rules_jvm_gazelle_setup")

contrib_rules_jvm_gazelle_setup()


##### RULES_JVM_EXTERNAL
RULES_JVM_EXTERNAL_TAG = "5.1"

RULES_JVM_EXTERNAL_SHA = "8c3b207722e5f97f1c83311582a6c11df99226e65e2471086e296561e57cc954"

http_archive(
    name = "rules_jvm_external",
    sha256 = RULES_JVM_EXTERNAL_SHA,
    strip_prefix = "rules_jvm_external-%s" % RULES_JVM_EXTERNAL_TAG,
    url = "https://github.com/bazelbuild/rules_jvm_external/releases/download/%s/rules_jvm_external-%s.tar.gz" % (RULES_JVM_EXTERNAL_TAG, RULES_JVM_EXTERNAL_TAG),
)

load("@rules_jvm_external//:repositories.bzl", "rules_jvm_external_deps")

rules_jvm_external_deps()

load("@rules_jvm_external//:setup.bzl", "rules_jvm_external_setup")

rules_jvm_external_setup()

load("@rules_jvm_external//:defs.bzl", "maven_install")

maven_install(
    artifacts = [
        "com.azure:azure-core:1.33.0",
        "com.azure:azure-identity:1.7.3",
        "com.azure:azure-security-keyvault-keys:4.5.1",
        "com.bettercloud:vault-java-driver:5.1.0",
        "com.fasterxml.jackson.core:jackson-databind:2.13.4.2",
        "com.fasterxml.jackson.datatype:jackson-datatype-guava:2.13.4",
        "com.fasterxml.jackson.datatype:jackson-datatype-jdk8:2.13.4",
        "com.fasterxml.jackson.datatype:jackson-datatype-joda:2.13.4",
        "com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.13.4",
        "com.fasterxml.jackson.module:jackson-module-parameter-names:2.13.4",
        "com.github.erosb:everit-json-schema:1.14.1",
        "com.google.api-client:google-api-client:1.35.2",
        "com.google.api.grpc:proto-google-common-protos:2.5.1",
        "com.google.crypto.tink:tink-awskms:1.8.0",
        "com.google.crypto.tink:tink-gcpkms:1.8.0",
        "com.google.crypto.tink:tink:1.8.0",
        "com.google.guava:guava-testlib:30.1.1-jre",
        "com.google.guava:guava:30.1.1-jre",
        "com.google.protobuf:protobuf-java-util:3.19.6",
        "com.google.protobuf:protobuf-java:3.19.6",
        "com.ibm.jsonata4java:JSONata4Java:2.2.5",
        "com.kjetland:mbknor-jackson-jsonschema_2.13:1.0.39",
        "com.squareup.okio:okio-jvm:3.0.0",
        "com.squareup.wire:wire-runtime-jvm:4.4.3",
        "com.squareup.wire:wire-schema-jvm:4.4.3",
        #        "io.confluent:assembly-plugin-boilerplate:resources",
        "io.confluent:common-config:7.5.0-355",
        "io.confluent:common-utils:7.5.0-355",
        "io.confluent:logredactor:1.0.11",
        "io.confluent:rest-utils:7.5.0-344",
        "io.kcache:kcache:4.0.11",
        "io.netty:netty-codec:4.1.86.Final",
        "io.swagger.core.v3:swagger-annotations:2.1.10",
        "io.swagger.core.v3:swagger-core:2.1.10",
        "javax.xml.ws:jaxws-api:2.3.0",
        #        "junit:junit:4.12",
        "junit:junit:4.13.2",
        "org.antlr:antlr4-runtime:4.11.1",
        "org.apache.avro:avro:1.11.0",
        "org.apache.commons:commons-compress:1.21",
        "org.apache.commons:commons-lang3:3.12.0",
        "org.apache.directory.api:api-all:1.0.0-M33",
        "org.apache.directory.server:apacheds-core-api:2.0.0-M22",
        "org.apache.directory.server:apacheds-interceptor-kerberos:2.0.0-M22",
        "org.apache.directory.server:apacheds-jdbm-partition:2.0.0-M22",
        "org.apache.directory.server:apacheds-ldif-partition:2.0.0-M22",
        "org.apache.directory.server:apacheds-mavibot-partition:2.0.0-M22",
        "org.apache.directory.server:apacheds-protocol-kerberos:2.0.0-M22",
        "org.apache.directory.server:apacheds-protocol-ldap:2.0.0-M22",
        "org.apache.directory.server:apacheds-protocol-shared:2.0.0-M22",
        "org.apache.kafka:connect-api:7.5.0-187-ccs",
        "org.apache.kafka:connect-file:7.5.0-187-ccs",
        "org.apache.kafka:connect-json:7.5.0-187-ccs",
        "org.apache.kafka:connect-runtime:7.5.0-187-ccs",
        "org.apache.kafka:kafka-clients:7.5.0-187-ccs",
        "org.apache.kafka:kafka-clients:jar:test:7.5.0-187-ccs",
        #        "org.apache.kafka:kafka-clients:test",
        "org.apache.kafka:kafka-streams:7.5.0-187-ccs",
        "org.apache.kafka:kafka_2.13:7.5.0-187-ccs",
        "org.apache.kafka:kafka_2.13:jar:test:7.5.0-187-ccs",
        #        "org.apache.kafka:kafka_2.13:test",
        "org.apache.maven.plugin-tools:maven-plugin-annotations:3.6.0",
        "org.apache.maven:maven-plugin-api:3.8.1",
        "org.apache.zookeeper:zookeeper:3.6.3",
        "org.bouncycastle:bcpkix-jdk15on:1.68",
        "org.easymock:easymock:4.3",
        "org.glassfish.jersey.ext:jersey-bean-validation:2.36",
        "org.hibernate.validator:hibernate-validator:6.1.7.Final",
        "org.jetbrains.kotlin:kotlin-scripting-compiler-embeddable:1.6.0",
        "org.jetbrains.kotlin:kotlin-scripting-jvm:1.6.0",
        "org.jetbrains.kotlin:kotlin-stdlib:1.6.0",
        "org.mockito:mockito-core:4.6.1",
        "org.mockito:mockito-inline:4.6.1",
        "org.openjdk.jmh:jmh-core:1.21",
        "org.openjdk.jmh:jmh-generator-annprocess:1.21",
        "org.powermock:powermock-module-junit4:2.0.9",
        "org.projectnessie.cel:cel-jackson:0.3.11",
        "org.projectnessie.cel:cel-tools:0.3.11",
        "org.slf4j:slf4j-reload4j:2.0.3",
        "org.yaml:snakeyaml:1.32",
        "uk.co.jemos.podam:podam:7.2.11.RELEASE",
    ],
    excluded_artifacts = [
        "xml-apis:xml-apis",
        "org.apache.directory.api:api-ldap-schema-data"
    ],
    maven_install_json = "//:maven_install.json",
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        "https://confluent-519856050701.d.codeartifact.us-west-2.amazonaws.com/maven/maven/",
        "https://confluent-519856050701.dp.confluent.io/maven/maven-public/",
        "https://packages.confluent.io/maven/",
        "https://jitpack.io",
        "https://oss.sonatype.org/content/repositories/snapshots",
        "https://repo1.maven.org/maven2",
        "https://repository.apache.org/snapshots",
        "https://maven.google.com",
    ],
    use_credentials_from_home_netrc_file = True,
)

load("@maven//:defs.bzl", "pinned_maven_install")

pinned_maven_install()

# for avro plugin
RULES_AVRO_VERSION = "a4c607a5610bea5649b1fb466ea8abcd9916121b"

RULES_AVRO_SHA256 = "aebc8fc6f8a6a3476d8e8f6f6878fc1cf7a253399e1b2668963e896512be1cc6"

http_archive(
    name = "io_bazel_rules_avro",
    sha256 = RULES_AVRO_SHA256,
    strip_prefix = "rules_avro-%s" % RULES_AVRO_VERSION,
    url = "https://github.com/chenrui333/rules_avro/archive/%s.tar.gz" % RULES_AVRO_VERSION,
)

load("@io_bazel_rules_avro//avro:avro.bzl", "avro_repositories")

avro_repositories(version = "1.11.0")

git_repository(
    name = "com_github_johnynek_bazel_jar_jar",
    commit = "78c8c13ff437e8397ffe80c9a4c905376720a339",
    remote = "https://github.com/johnynek/bazel_jar_jar.git",
)

load(
    "@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl",
    "jar_jar_repositories",
)

jar_jar_repositories()

# for jmh benchmark
http_archive(
    name = "rules_jmh",
    sha256 = "dbb7d7e5ec6e932eddd41b910691231ffd7b428dff1ef9a24e4a9a59c1a1762d",
    strip_prefix = "buchgr-rules_jmh-6ccf8d7",
    type = "zip",
    url = "https://github.com/buchgr/rules_jmh/zipball/6ccf8d7b270083982e5c143935704b9f3f18b256",
)

load("@rules_jmh//:deps.bzl", "rules_jmh_deps")

rules_jmh_deps()

load("@rules_jmh//:defs.bzl", "rules_jmh_maven_deps")

rules_jmh_maven_deps()

# for maven-assembly-plugin.
http_archive(
    name = "rules_pkg",
    sha256 = "335632735e625d408870ec3e361e192e99ef7462315caa887417f4d88c4c8fb8",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
        "https://github.com/bazelbuild/rules_pkg/releases/download/0.9.0/rules_pkg-0.9.0.tar.gz",
    ],
)

load("@rules_pkg//:deps.bzl", "rules_pkg_dependencies")

rules_pkg_dependencies()

# for googleapis
http_archive(
    name = "googleapis",
    build_file = "@//:BUILD.google",
    strip_prefix = "googleapis-common-protos-1_3_1/",
    url = "https://github.com/googleapis/googleapis/archive/common-protos-1_3_1.zip",
)

maven_install(
    name = "swagger_deps",
    artifacts = [
        "io.swagger.core.v3:swagger-core:2.2.9",
        "io.swagger.core.v3:swagger-integration:2.2.9",
        "io.swagger.core.v3:swagger-jaxrs2:2.2.9",
        "org.codehaus.plexus:plexus-utils:3.5.1",

        # reflectively loaded by swagger-core
        "javax.ws.rs:javax.ws.rs-api:2.1.1",
        "javax.servlet:javax.servlet-api:4.0.1",
    ],
    # maven_install_json = "//:maven_install.json",
    repositories = [
        # Private repositories are supported through HTTP Basic auth
        "https://confluent-519856050701.d.codeartifact.us-west-2.amazonaws.com/maven/maven/",
        "https://confluent-519856050701.dp.confluent.io/maven/maven-public/",
        "https://packages.confluent.io/maven/",
        "https://jitpack.io",
        "https://oss.sonatype.org/content/repositories/snapshots",
        "https://repo1.maven.org/maven2",
        "https://repository.apache.org/snapshots",
        "https://maven.google.com",
    ],
    use_credentials_from_home_netrc_file = True,
)



#local_repository(
#    name = "confluent_rules",
#    path = "/Users/vrose/dev/confluentinc/confluent-bazel-rules",
#)
#
#load("@confluent_rules//swagger-core-bazel:deps.bzl", "swagger_deps")
#swagger_deps()
