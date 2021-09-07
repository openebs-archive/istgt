# Code Standards

## Sign your work

We use the Developer Certificate of Origin (DCO) as an additional safeguard for the OpenEBS project. This is a well established and widely used mechanism to assure that contributors have confirmed their right to license their contribution under the project's license. Please read [dcofile](https://github.com/openebs/openebs/blob/main/contribute/developer-certificate-of-origin). If you can certify it, then just add a line to every git commit message:

```
  Signed-off-by: Random J Developer <random@developer.example.org>
```

Use your real name (sorry, no pseudonyms or anonymous contributions). The email id should match the email id provided in your GitHub profile.
If you set your `user.name` and `user.email` in git config, you can sign your commit automatically with `git commit -s`.

You can also use git [aliases](https://git-scm.com/book/tr/v2/Git-Basics-Git-Aliases) like `git config --global alias.ci 'commit -s'`. Now you can commit with `git ci` and the commit will be signed.

## Verifying code style

All the new changes to this repository need to follow coding style as per `./cstyle.pl` script. But, due to many legacy files which are not having coding style as per ./cstyle.pl script, make cstyle fails.
So, verify the lint errors by running `./cstyle.pl <filename_with_path>` on the modified files to make sure that new modifications meets the coding style.

## Adding a changelog
If PR is about adding a new feature or bug fixes then Authors of the PR are expected to add a changelog file with their pull request. This changelog file should be a new file created under `changelogs/unreleased` folder. Name of this file must be in in `pr_number-username` format and contents of the file should be the one liner text which explain the feature or bug fix.

```sh
istgt/changelogs/unreleased   <- folder
    12-github_user_name            <- file
```
