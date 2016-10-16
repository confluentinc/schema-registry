The following commands were used to generate license and notice files. Replace <VERSION> with
the Schema Registry version, <SRC_PATH> with the path to the Schema Registry source directory,
and <LICENSE_TOOL_PATH> with the path of the license tool.

cd <SRC_PATH>
mvn package -DskipTests
mkdir /tmp/jars
mkdir /tmp/overrides
cp package-schema-registry/target/kafka-schema-registry-package-<VERSION>-package/share/java/schema-registry/*.jar /tmp/jars/
cp package-kafka-serde-tools/target/kafka-serde-tools-package-<VERSION>-package/share/java/kafka-serde-tools/*.jar /tmp/jars/
cd <LICENSE_TOOL_PATH>
./bin/run_license_job.bash -i /tmp/jars -l <SRC_PATH>/licenses -n <SRC_PATH>/notices -h <SRC_PATH>/licenses-and-notices.html -o /tmp/overrides
