#ifndef _REPLICATION_MISC_H
#define	_REPLICATION_MISC_H

#include <json-c/json_object.h>
#include "istgt_misc.h"

int replication_connect(const char *, int);
int replication_listen(const char *, int, int, int);
int set_socket_keepalive(int);
json_object * json_object_new_uint64(uint64_t value);

#endif /* _REPLICATION_MISC_H */
