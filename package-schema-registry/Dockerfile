#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds a docker image for the Confluent Schema Registry.

ARG DOCKER_UPSTREAM_REGISTRY
ARG DOCKER_UPSTREAM_TAG

FROM ${DOCKER_UPSTREAM_REGISTRY}confluentinc/cp-base:${DOCKER_UPSTREAM_TAG}

ARG PROJECT_VERSION
ARG ARTIFACT_ID

ARG COMMIT_ID=unknown
LABEL io.confluent.docker.git.id=$COMMIT_ID
ARG BUILD_NUMBER=-1
LABEL io.confluent.docker.build.number=$BUILD_NUMBER

MAINTAINER partner-support@confluent.io
LABEL io.confluent.docker=true

ENV COMPONENT=schema-registry

# Default listener
EXPOSE 8081

RUN echo "===> Setting up ${COMPONENT} dirs" \
    && mkdir -p /etc/${COMPONENT}/secrets\
    && chmod -R ag+w /etc/${COMPONENT} /etc/${COMPONENT}/secrets

VOLUME ["/etc/${COMPONENT}/secrets"]

COPY include/etc/confluent/docker /etc/confluent/docker

ADD target/kafka-schema-registry-package-${PROJECT_VERSION}-package/share/java/kafka-schema-registry-package/* /usr/share/java/${ARTIFACT_ID}/
ADD target/kafka-schema-registry-package-${PROJECT_VERSION}-package/share/doc/* /usr/share/doc/${ARTIFACT_ID}/
ADD target/kafka-schema-registry-package-${PROJECT_VERSION}-package/bin/* /usr/bin/
ADD target/kafka-schema-registry-package-${PROJECT_VERSION}-package/etc/* /etc/

ENV CLASSPATH=/usr/share/java/${ARTIFACT_ID}/*

CMD ["/etc/confluent/docker/run"]
