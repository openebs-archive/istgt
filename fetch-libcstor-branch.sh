#!/bin/bash
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

