FROM maven:3.5.4-jdk-8 as build

ENV MAVEN_FLAGS="-Dmaven.test.skip=true -Dmaven.repo.local=/root/.m2_novolume" \
    CP_ARCHIVE_VERSION=v5.1.0 \
    CP_PACKAGE_VERSION=5.1.0

# Confluent build dependencies

RUN mkdir -p /opt/src/common; cd /opt/src/common; \
  curl -SLs https://github.com/confluentinc/common/archive/$CP_ARCHIVE_VERSION.tar.gz | tar -xzf - --strip-components=1 -C ./; \
  mvn $MAVEN_FLAGS install

RUN mkdir -p /opt/src/rest-utils; cd /opt/src/rest-utils; \
  curl -SLs https://github.com/confluentinc/rest-utils/archive/$CP_ARCHIVE_VERSION.tar.gz | tar -xzf - --strip-components=1 -C ./; \
  mvn $MAVEN_FLAGS install

# General dependencies

RUN mkdir -p /opt/src/schema-registry; cd /opt/src/schema-registry; \
  mkdir -p core package-schema-registry maven-plugin package-kafka-serde-tools avro-serde/src/main/avro avro-console-scripts avro-converter/src/main/avro json-serializer avro-serializer/src/main/avro schema-registry-console-scripts client

WORKDIR /opt/src/schema-registry

COPY pom.xml .
COPY core/pom.xml core/
COPY package-schema-registry/pom.xml package-schema-registry/
COPY maven-plugin/pom.xml maven-plugin/
COPY package-kafka-serde-tools/pom.xml package-kafka-serde-tools/
COPY avro-serde/pom.xml avro-serde/
COPY avro-console-scripts/pom.xml avro-console-scripts/
COPY avro-converter/pom.xml avro-converter/
COPY json-serializer/pom.xml json-serializer/
COPY avro-serializer/pom.xml avro-serializer/
COPY schema-registry-console-scripts/pom.xml schema-registry-console-scripts/
COPY client/pom.xml client/

RUN mvn $MAVEN_FLAGS -Dcheckstyle.skip compile

# And finally, build the project

COPY . /opt/src/schema-registry

RUN mvn $MAVEN_FLAGS install

# Remove optional transitive dependency that has a vulnerability (CVE-2013-2035):
RUN rm -f /opt/src/common/package/target/common-package-$CP_PACKAGE_VERSION-package/share/java/confluent-common/jline-0.9.94.jar


###############

# We need a glibc-based Alpine build since RocksDB's JNI libs depend on glibc.
FROM anapsix/alpine-java:8u192b12_jdk_unlimited

ENV CP_PACKAGE_VERSION=5.0.1

RUN mkdir -p /usr/local/share/java
RUN mkdir -p /etc/kafka

COPY --from=build \
    /opt/src/common/package/target/common-package-$CP_PACKAGE_VERSION-package/share/java/confluent-common \
    /usr/local/share/java/confluent-common

COPY --from=build \
    /opt/src/rest-utils/package/target/rest-utils-package-$CP_PACKAGE_VERSION-package/share/java/rest-utils \
    /usr/local/share/java/rest-utils

COPY --from=build \
    /opt/src/schema-registry/package-schema-registry/target/kafka-schema-registry-package-${CP_PACKAGE_VERSION}-package/share/java \
    /usr/local/share/java/

COPY --from=build \
    /opt/src/schema-registry/package-schema-registry/target/kafka-schema-registry-package-$CP_PACKAGE_VERSION-package/bin \
    /usr/local/bin/

COPY --from=build \
    /opt/src/schema-registry/package-schema-registry/target/kafka-schema-registry-package-$CP_PACKAGE_VERSION-package/etc \
    /usr/local/etc/

RUN adduser -D -g '' sunbit
RUN chown -R sunbit /etc/kafka
RUN chown -R sunbit /tmp
USER sunbit

CMD ["schema-registry-start", "/etc/kafka/schema-registry.properties"]
