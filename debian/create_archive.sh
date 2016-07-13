#!/bin/bash
#
# Creates an archive suitable for distribution (standard layout for binaries,
# libraries, etc.).

set -e

if [ -z ${PACKAGE_TITLE} -o -z ${VERSION} -o -z ${DESTDIR} ]; then
    echo "PACKAGE_TITLE, VERSION, and DESTDIR environment variables must be set."
    exit 1
fi

BINPATH=${PREFIX}/bin
LIBPATH=${PREFIX}/share/java/${PACKAGE_TITLE}
DOCPATH=${PREFIX}/share/doc/${PACKAGE_TITLE}

INSTALL="install -D -m 644"
INSTALL_X="install -D -m 755"

rm -rf ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${PREFIX}
mkdir -p ${DESTDIR}${BINPATH}
mkdir -p ${DESTDIR}${LIBPATH}
mkdir -p ${DESTDIR}${SYSCONFDIR}
mkdir -p ${DESTDIR}${SYSTEMDDIR}

# schema registry
PREPACKAGED_SCHEMA_REGISTRY="package-schema-registry/target/kafka-schema-registry-package-${VERSION}-package"
pushd ${PREPACKAGED_SCHEMA_REGISTRY}
find bin/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL_X} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
find share/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
pushd etc/schema-registry/
find . -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${SYSCONFDIR}/XXX
popd
popd

find systemd -type f | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${SYSTEMDDIR}/XXX

# kafka-serde-tools
PREPACKAGED_KAFKA_SERDE_TOOLS="package-kafka-serde-tools/target/kafka-serde-tools-package-${VERSION}-package"
pushd ${PREPACKAGED_KAFKA_SERDE_TOOLS}
find bin/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL_X} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
find share/ -type f | grep -v README[.]rpm | xargs -I XXX ${INSTALL} -o root -g root XXX ${DESTDIR}${PREFIX}/XXX
popd
