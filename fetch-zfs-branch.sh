#!/bin/bash
set -e

#This file is used to specify the ZFS branch from
# where to fetch the zfs header files. Depending 
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
# v0.7.x. This file needs to determine the correct zfs branch
# by finding the parent of `fix-ta123` and checking if the 
# correpsonding branch exists on openebs/zfs.
#
#For the moment, we will go with making sure the correct
# branch name is provided as part of the release process.
ZFS_BUILD_BRANCH="zfs-0.7-release"
export ZFS_BUILD_BRANCH

