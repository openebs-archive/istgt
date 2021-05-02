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

# Running the tests

sudo bash ./print_debug_info.sh &
## If test takes more than 45 minutes then print tail logs in /tmp/istgt.log and terminate travis
## Having 60minutes is causing travis timeout and not able to know easily which test is failed
sudo bash ./test_istgt.sh 
RESULT=$?
if [ $RESULT -ne 0 ]; then
    sudo tail -500 /tmp/istgt.log && exit 1 
fi
