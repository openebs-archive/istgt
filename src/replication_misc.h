/*
 * Copyright © 2017-2019 The OpenEBS Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _REPLICATION_MISC_H
#define	_REPLICATION_MISC_H

#include <json-c/json_object.h>
#include "istgt_misc.h"

int replication_connect(const char *, int);
int replication_listen(const char *, int, int, int);
int set_socket_keepalive(int);
json_object * json_object_new_uint64(uint64_t value);

#endif /* _REPLICATION_MISC_H */
