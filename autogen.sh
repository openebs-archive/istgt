#!/bin/bash

source ./fetch-libcstor-branch.sh

echo "Using libcstor branch - ${LIBCSTOR_BUILD_BRANCH}"
echo $(wget -O /tmp/zrepl_prot.h https://raw.githubusercontent.com/openebs/libcstor/${LIBCSTOR_BUILD_BRANCH}/include/zrepl_prot.h)

autoreconf -fiv
rm -Rf autom4te.cache
