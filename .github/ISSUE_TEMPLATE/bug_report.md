---
name: Bug Report
about: Report a bug encountered while operating on CStor Volumes
labels: kind/bug

---

<!-- Please use this template while reporting a bug and provide as much info as possible. Not doing so may result in your bug not being addressed in a timely manner. Thanks!
-->

**What happened:**

**What you expected to happen**:

**The output of the following commands will help us better understand what's going on**:
(Pasting long output into a [GitHub gist](https://gist.github.com) or other [Pastebin](https://pastebin.com/) is fine)

* `kubectl get cstorvolume <pv-name> -n <openebs_namespace> -o yaml`
* `kubectl get cvr -l openebs.io/persistent-volume=<pv_name> -o yaml`
* `kubectl get csp -l openebs.io/storage-pool-claim=<spc_name> -o yaml`
* `kubectl logs deployment/<pv-name>-target -n <openebs_namespace> -c cstor-istgt`
* `kubectl logs deployment/<pv-name>-target -n <openebs_namespace> -c cstor-volume-mgmt`
* `kubectl get pods -n <openebs_namespace>`

**How to reproduce it (as minimally and precisely as possible):**

**Anything else we need to know?:**

**Environment:**
- OpenEBS volume version (use `kubectl get po -n openebs --show-labels`):
- Cloud provider or hardware configuration:
- OS (e.g: `cat /etc/os-release`):
- kernel (e.g: `uname -a`):
- others:
