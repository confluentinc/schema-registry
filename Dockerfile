FROM us.gcr.io/container-registry-147718/clover_node:master

RUN yum -y install java

RUN mkdir /schema-registry
COPY . /schema-registry/

CMD schema-registry/bin/schema-registry-start schema-registry/config/schema-registry.properties

