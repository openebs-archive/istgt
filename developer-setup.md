
# Development Workflow

## Prerequisites

* Since istgt works with Linux only, you need to have a working Linux machine
* Make sure that GCC, with version >6 is installed in your system.
  To install GCC, run
  ```sh
  sudo apt-get install --yes -qq gcc-6 g++-6
  ```
* Make sure that you have installed following packages in your system:
    - libssl-dev, open-iscsi, libjson-c-dev, ioping, jq and net-tools
  To install the above packages, run
  ```sh
  sudo apt-get install libssl-dev open-iscsi libjson-c-dev ioping jq net-tools
  ```

## Initial Setup

### Fork in the cloud

1. Visit https://github.com/openebs/istgt
2. Click `Fork` button (top right) to establish a cloud-based fork.

### Clone fork to local host

Create your clone:

```sh
# Note: Here user= your github profile name
git clone https://github.com/$user/istgt.git

# Configure remote upstream
cd istgt
git remote add upstream https://github.com/openebs/istgt.git

# Never push to upstream replication
git remote set-url --push upstream no_push

# Confirm that your remotes make sense
git remote -v
```

### Instructions to check cstyle
```
Checkout replication branch
Do ./cstyle.pl <filename with path>
```

### Building and Testing your changes

* To build the istgt binary
```sh
./autogen.sh
./configure --enable-replication
make cstyle
make clean
make
```

* To build the docker image
```sh
./build_image.sh
```

* Test your changes
Most of the test cases are written in shell script and run in a local machine.
To run the test on a local machine
```sh
sudo bash test_istgt.sh
```

## Git Development Workflow

### Always sync your local repository:
Open a terminal on your local host. Change directory to the istgt fork root.

```sh
$ cd <path_to_fork_repo>
```

 Checkout the replication branch.

 ```sh
 $ git checkout replication
 Switched to branch 'replication'
 Your branch is up-to-date with 'origin/replication'.
 ```

 Recall that origin/replication is a branch on your remote GitHub repository.
 Make sure you have the upstream remote openebs/istgt by listing them.

 ```sh
 $ git remote -v
 origin	https://github.com/$user/istgt.git (fetch)
 origin	https://github.com/$user/istgt.git (push)
 upstream	https://github.com/openebs/istgt.git (fetch)
 upstream	https://github.com/openebs/istgt.git (no_push)
 ```

 If the upstream is missing, add it by using below command.

 ```sh
 $ git remote add upstream https://github.com/openebs/istgt.git
 ```
 Fetch all the changes from the upstream replication branch.

 ```sh
 $ git fetch upstream replication
 From https://github.com/openebs/istgt
 * branch            replication -> FETCH_HEAD
 ```

 Rebase your local replication branch with the upstream/replication.

 ```sh
 $ git rebase upstream/replication
 First, rewinding head to replay your work on top of it...
 Fast-forwarded replication to upstream/replication.
 ```
 This command applies all the commits from the upstream replication to your local development branch.

 Check the status of your local branch.

 ```sh
 $ git status
 On branch develop
 nothing to commit, working directory clean
 ```
 Your local repository now has all the changes from the upstream remote. You need to push the changes to your own remote fork which is origin replication.

 Push the rebased replication to origin replication.

 ```sh
 $ git push origin replication
 Username for 'https://github.com': $user
 Password for 'https://$user@github.com':
 Counting objects: 223, done.
 Compressing objects: 100% (38/38), done.
 Writing objects: 100% (69/69), 8.76 KiB | 0 bytes/s, done.
 Total 69 (delta 53), reused 47 (delta 31)
 To https://github.com/$user/istgt.git
 8e107a9..5035fa1  replication -> replication
 ```

### Contributing to a feature or bugfix.

Always start with creating a new branch from replication to work on a new feature or bugfix. Your branch name should have the format XX-descriptive where XX is the issue number you are working on followed by some descriptive text. For example:

 ```sh
 $ git checkout replication
 # Make sure the replication is rebased with the latest changes as described in previous step.
 $ git checkout -b 1234-fix-developer-docs
 Switched to a new branch '1234-fix-developer-docs'
 ```
Happy Hacking!

### Keep your branch in sync

[Rebasing](https://git-scm.com/docs/git-rebase) is very import to keep your branch in sync with the changes being made by others and to avoid huge merge conflicts while raising your Pull Requests. You will always have to rebase before raising the PR.

```sh
# While on your myfeature branch (see above)
git fetch upstream
git rebase upstream/replication
```

While you rebase your changes, you must resolve any conflicts that might arise and build and test your changes using the above steps.

## Submission

### Create a pull request

Before you raise the Pull Requests, ensure you have reviewed the checklist in the [CONTRIBUTING GUIDE](../CONTRIBUTING.md):
- Ensure that you have re-based your changes with the upstream using the steps above.
- Ensure that you have added the required unit tests for the bug fixes or new feature that you have introduced.
- Ensure your commits history is clean with proper header and descriptions.

Go to the [openebs/istgt github](https://github.com/openebs/istgt) and follow the Open Pull Request link to raise your PR from your development branch.
