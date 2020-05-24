#!/bin/bash
# Copyright 2019-2020 The OpenEBS Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


source ./fetch-libcstor-branch.sh

REPO_ORG="openebs"
if [ ! -z $TRAVIS_REPO_SLUG ]; then
	REPO_ORG=$(echo "$TRAVIS_REPO_SLUG" | cut -d'/' -f1);
fi

echo "Using libcstor branch - ${LIBCSTOR_BUILD_BRANCH}"
echo $(wget -O /tmp/zrepl_prot.h https://raw.githubusercontent.com/${REPO_ORG}/libcstor/${LIBCSTOR_BUILD_BRANCH}/include/zrepl_prot.h)

autoreconf -fiv
rm -Rf autom4te.cache
