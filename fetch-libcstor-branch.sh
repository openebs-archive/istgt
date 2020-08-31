#!/bin/bash

## Copyright © 2019 The OpenEBS Authors
## 
## Licensed under the Apache License, Version 2.0 (the "License");
## you may not use this file except in compliance with the License.
## You may obtain a copy of the License at
## 
##     http://www.apache.org/licenses/LICENSE-2.0
## 
## Unless required by applicable law or agreed to in writing, software
## distributed under the License is distributed on an "AS IS" BASIS,
## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
## See the License for the specific language governing permissions and
## limitations under the License.

set -e

#This file is used to specify the LibCStor branch from
# where to fetch the zrepl header files. Depending
# on the release - the header file should be fetched
# from different location.
#
# For example:
#  master -> master
#  v0.7.x -> v0.7.x
#
# The above 1:1 mapping can't always be guaranteed when
# working in forked repos with branch names like:
#  fix-ta123
#
# Now, `fix-ta123` could be a created either from master or
# v0.7.x. This file needs to determine the correct libcstor branch
# by finding the parent of `fix-ta123` and checking if the
# correpsonding branch exists on openebs/libcstor.
#
#For the moment, we will go with making sure the correct
# branch name is provided as part of the release process.
if [ -z ${TRAVIS_BRANCH} ] ||
    [ ${TRAVIS_BRANCH} == "replication" ]; then
    LIBCSTOR_BUILD_BRANCH="master"
else
    LIBCSTOR_BUILD_BRANCH=${TRAVIS_BRANCH}
fi

export LIBCSTOR_BUILD_BRANCH

