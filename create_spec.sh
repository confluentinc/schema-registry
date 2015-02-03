#!/bin/bash

# create_spec.sh spec.in out.spec

set -e

RELEASE_FILE="RELEASE_${RPM_VERSION}${RPM_RELEASE_POSTFIX_UNDERSCORE}"
echo "Creating the spec file"
read RPM_RELEASE_ID < ${RELEASE_FILE}
(( RPM_RELEASE_ID += 1))
echo ${RPM_RELEASE_ID} > ${RELEASE_FILE}
if [ -n "${RPM_RELEASE_POSTFIX}" ]; then
    RPM_RELEASE_ID="0.${RPM_RELEASE_ID}.${RPM_RELEASE_POSTFIX}"
fi

cat $1 | sed "s@##RPMVERSION##@${RPM_VERSION}@g; s@##RPMRELEASE##@${RPM_RELEASE_ID}@g" > $2
