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

#ifndef	_RING_MEMPOOL_H
#define	_RING_MEMPOOL_H

#include <stdbool.h>
#include "rte_ring.h"

typedef int mempool_constructor_t(void *, void *, int);
typedef void mempool_destructor_t(void *, void *);
typedef void mempool_reclaim_t(void *);

typedef struct {
	unsigned length;
	size_t entry_offset;
	struct rte_ring *ring;
	mempool_constructor_t *create;
	mempool_destructor_t *free;
	mempool_reclaim_t *reclaim;
} rte_smempool_t;

int init_mempool(rte_smempool_t *obj, size_t count, size_t mem_size,
    size_t offset, const char *mempool_name, mempool_constructor_t *create,
    mempool_destructor_t *free, mempool_reclaim_t *reclaim, bool initialize);
int destroy_mempool(rte_smempool_t *obj);
void * get_from_mempool(rte_smempool_t *obj);
void put_to_mempool(rte_smempool_t *obj, void *node);
unsigned get_num_entries_from_mempool(rte_smempool_t *obj);

#ifdef HAVE_CONFIG_H
#include "config.h"

#ifdef REPLICATION
#include "replication.h"
#endif

#endif

#ifndef REPLICATION
#define	REPLICA_LOG
#define	REPLICA_NOTICELOG
#define	REPLICA_ERRLOG
#define	REPLICA_WARNLOG
#endif

#endif /* _RING_MEMPOOL_H */
