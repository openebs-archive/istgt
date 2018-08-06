#ifndef _REPLICATION_MISC_H
#define	_REPLICATION_MISC_H

#include "istgt_misc.h"

int replication_connect(const char *, int);
int replication_listen(const char *, int, int, int);
int set_socket_keepalive(int);

#endif /* _REPLICATION_MISC_H */
