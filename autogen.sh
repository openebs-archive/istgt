#!/bin/sh

if [ ! -z ${TRAVIS_TAG} ];
then
  ZFS_BUILD_BRANCH=${TRAVIS_TAG}
else
  . ./fetch-zfs-branch.sh
fi

echo "Using zfs branch - ${ZFS_BUILD_BRANCH}"
echo $(wget -O /tmp/zrepl_prot.h https://raw.githubusercontent.com/openebs/zfs/${ZFS_BUILD_BRANCH}/include/zrepl_prot.h)

autoreconf -fiv
rm -Rf autom4te.cache
