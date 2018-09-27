#!/bin/bash
set -e

if [ ! -z ${TRAVIS_TAG} ];
then
  ZFS_BUILD_BRANCH=${TRAVIS_TAG}
  echo "Using ZFS repo tag: ${ZFS_BUILD_BRANCH}"
else
  #Current active (master) zfs and spl development branch
  ZFS_MASTER="zfs-0.7-release"
  ZFS_REPO="https://github.com/openebs/zfs.git"

  #Determinte the current branch depending on the build env.
  CURRENT_BRANCH=""
  if [ -z ${TRAVIS_BRANCH} ];
  then
    CURRENT_BRANCH=$(git branch | grep \* | cut -d ' ' -f2)
  else
    CURRENT_BRANCH=${TRAVIS_BRANCH}
  fi

  #Depending on the current istgt branch name decide corresponding ZFS Build branch.
  # If current is master - use $ZFS_MASTER
  # If current is non master branch and if it exists on remote use non-master branch
  #   else set it to $ZFS_MASTER
  ZFS_BUILD_BRANCH=${ZFS_MASTER}
  if [ -z ${CURRENT_BRANCH} ] || [ ${CURRENT_BRANCH} = "replication" ];
  then
    echo "Using ZFS master build branch: ${ZFS_BUILD_BRANCH}"
  else
    #If the current branch exists on the remote repo, use it for building
    # Branch names can be really funky with substring matches.
    # Extract the branch name from git ls-remote. The output line will be like:
    #  a8577bdb32e091645df901d8501e44ef50748389        refs/heads/master
    #  3be04072fa507127e9161283f27024e2bde702e3        refs/heads/rebuild_snapshot
    #  33218f9966a12f8961f7d7e94307a74328acf928        refs/heads/rebuild_snapshot_1
    BF=$(git ls-remote --heads ${ZFS_REPO} | cut -d '/' -f3 | grep -x "${CURRENT_BRANCH}" | wc -l)
    if [ $BF -ne 0 ]; then
      ZFS_BUILD_BRANCH=${CURRENT_BRANCH}
      echo "Using ZFS build branch: ${ZFS_BUILD_BRANCH}"
    else
      echo "${CURRENT_BRANCH} not found on zfs repo."
      echo "Using ZFS master build branch: ${ZFS_BUILD_BRANCH}"
    fi
  fi
fi

export ZFS_BUILD_BRANCH

