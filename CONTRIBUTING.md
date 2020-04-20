# Contributing to OpenEBS istgt

istgt uses the standard GitHub pull requests process to review and accept contributions.

You can contribute to istgt by filling a issue at [openebs/istgt](https://github.com/openebs/istgt/issues) or submitting a pull reuqest to this repository.
* If you want to file a issue for bug or feature request, please see [Filing a issue](#filing-a-issue)
* If you are a first-time contributor, please see [Steps to Contribute](#steps-to-contribute) and code standard(code-standard.md).
* If you would like to work on something more involved, please connect with the OpenEBS Contributors. See [OpenEBS Community](https://github.com/openebs/openebs/tree/master/community)
## Filing a issue
### Before filing an issue

If you are unsure whether you have found a bug, please consider asking in the [Slack](https://kubernetes.slack.com/messages/openebs) first. If the behavior you are seeing is confirmed as a bug or issue, it can easily be re-raised in the [issue tracker](https://github.com/openebs/istgt/issues).

### Filing issues

When filing an issue, make sure to answer the following questions:

1. What version of OpenEBS are you using?
2. What did you expect to see?
3. What did you see instead?
4. How to reproduce it?
5. Logs of cstor-istgt and cstor-volume-mgmt containerss of cstor target pod.

## Steps to Contribute

istgt is an Apache 2.0 Licensed project and all your commits should be signed with Developer Certificate of Origin. See [Sign your work](./code-standard.md).


* Find an issue to work on or create a new issue. The issues are maintained at [openebs/istgt](https://github.com/openebs/istgt/issues). You can pick up from a list of [good-first-issues](https://github.com/openebs/openebs/labels/good%20first%20issue).
* Claim your issue by commenting your intent to work on it to avoid duplication of efforts.
* Fork the repository on GitHub.
* Create a branch from where you want to base your work from replication branch.
* Make your changes. If you are working on code contributions, please see [Setting up the Development Environment](#setting-up-your-development-environment).
* Commit your changes by making sure the commit messages convey the need and notes about the commit.
* Please make sure than your code is aligned with the standard mentioned at [code-standard](code-standard.md).
* Verify that your changes passes `sudo bash test_istgt.sh`
* Push your changes to the branch in your fork of the repository.
* Submit a pull request to the original repository. See [Pull Request checklist](#pull-request-checklist).

## Pull Request Checklist
* Rebase to the current replication branch before submitting your pull request.
* Commits should be as small as possible. Each commit should follow the checklist below:
  - For code changes, add tests relevant to the fixed bug or new feature.
  - Pass the compile and tests - includes spell checks, formatting, etc.
  - Commit header (first line) should convey what changed.
  - Commit body should include details such as why the changes are required and how the proposed changes help.
  - DCO Signed, please refer [signing commit](code-standard.md/sign-your-commits)
* If your PR is about fixing a issue or new feature, make sure you add a change-log. Refer [Adding a Change log](code-standard.md/adding-a-changelog)
* PR title must follow convention: `<type>(<scope>): <subject>`.

For example:
  ```
   feat(replication): support for enabling replication
   ^--^ ^-----^   ^-----------------------^
     |     |         |
     |     |         +-> PR subject, summary of the changes
     |     |
     |     +-> scope of the PR, i.e. component of the project this PR is intend to update
     |
     +-> type of the PR.
  ```
  Most common types are:
    * `feat`        - for new features, not a new feature for build script
    * `fix`         - for bug fixes or improvements, not a fix for build script
    * `chore`       - changes not related to production code
    * `docs`        - changes related to documentation
    * `style`       - formatting, missing semi colons, linting fix etc; no significant production code changes
    * `test`        - adding missing tests, refactoring tests; no production code change
    * `refactor`    - refactoring production code, eg. renaming a variable or function name, there should not be any significant production code changes
    * `cherry-pick` - if PR is merged in master branch and raised to release branc

## Code Reviews
All submissions, including submissions by project members, require review. We use GitHub pull requests for this purpose. Consult [GitHub Help](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests) for more information on using pull requests.

* If your PR is not getting reviewed or you need a specific person to review it, please reach out to the OpenEBS Contributors. See [OpenEBS Community](https://github.com/openebs/openebs/tree/master/community)

* If PR is fixing any issues from [github-issues](github.com/openebs/istgt/issues) then you need to mention the issue number with link in PR description. like : _fixes https://github.com/openebs/istgt/issues/56_

* If PR is for bug-fix and release branch(like v1.9.x) is created then cherry-pick for the same PR needs to be created against the release branch. Maintainer of the Project needs to make sure that all the bug fix after RC release are cherry-picked to release branch.

### For maintainers
* We are using labelling for PR to track it more effectively. Following are valid labels for the PR.
   - **Bug** - if PR is a **bug to existing feature**
   - **Enhancement** - if PR is a **feature request**
   - **Maintenance**  - if PR is not related to production code. **build, document or test related PR falls into this category**
   - **Documentation** - if PR is about **tracking the documentation work for the feature**. This label should not be applied to the PR fixing bug in documentations.

* We are using following label for PR work-flow:
   - **Hold-Merge** - if PR needs to complete few activities.
   - **Hold-Review** - if PR is under Work In Progress, not yet covered all the scenarios.
   - **Release-note** - if PR required to mention in release note.

* Maintainer needs to make sure that appropriate milestone and project tracker is assigned to the PR.

**If you want to introduce a new label then you need to raise a PR to update this document with the new label details.**<Paste>
