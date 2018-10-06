#ifdef HAVE_CONFIG_H
#include "config.h"

#ifdef REPLICATION
#include "replication.h"
#endif

#endif

#include "ring_mempool.h"
#include "assert.h"

int
init_mempool(rte_smempool_t *obj, size_t count, size_t mem_size,
    size_t offset, const char *ring_name, mempool_constructor_t *mem_init,
    mempool_destructor_t *mem_remove, mempool_reclaim_t *mem_reclaim,
    bool initialize)
{
	size_t i = 0;
	void *mem_entry = NULL;
	int rc = 0;

	ASSERT(count);

	obj->entry_offset = offset;
	obj->ring = rte_ring_create(ring_name, count, -1, RING_F_EXACT_SZ);
	obj->create = mem_init;
	obj->free = mem_remove;
	obj->reclaim = mem_reclaim;

	if (!initialize) {
		ASSERT0(mem_size);
		obj->length = 0;
	} else {
		for (i = 0; i < count; i++, mem_entry = NULL) {
			mem_entry = malloc(mem_size);
			if (!mem_entry) {
				REPLICA_ERRLOG("failed to allocate memory for "
				    "mempool(%s)'s entry\n", obj->ring->name);
				rc = -1;
				break;
			}

			rc = rte_ring_enqueue(obj->ring, mem_entry);
			if (rc) {
				REPLICA_ERRLOG("failed to insert entry in "
				    "mempool(%s).. \n", obj->ring->name);
				rc = -1;
				break;
			}
		}

		obj->length = i;

		/*
		 * If we are unable to get provided number of entries in mempool
		 * then we will return
		 *	(-1) if we couldn't add single entry in mempool
		 *	(1)  if there are some entries in mempool
		 *  that caller may use
		 */
		if (rc) {
			if (i)
				rc = 1;
			else
				rc = -1;
		}
	}

	return (rc);
}


int
destroy_mempool(rte_smempool_t *obj)
{
	size_t i = 0;
	void *entry;
	int rc;

	if (!obj || !obj->ring)
		return (0);

	if (rte_ring_count(obj->ring) != obj->length) {
		REPLICA_ERRLOG("there are still orphan entries(%d) for "
		    "mempool(%s)\n",
		    obj->length - rte_ring_count(obj->ring), obj->ring->name);
		return (obj->length - rte_ring_count(obj->ring));
	}

	for (i = 0; i < obj->length; i++) {
		rc = rte_ring_dequeue(obj->ring, &entry);
		if (rc)
			break;
		free(entry);
	}

	ASSERT0(rte_ring_count(obj->ring));
	rte_ring_free(obj->ring);
	obj->ring = NULL;
	return (0);
}

void *
get_from_mempool(rte_smempool_t *obj)
{
	void *entry = NULL;
	int rc = 0;
	int count = 0;

	do {
		rc = rte_ring_dequeue(obj->ring, &entry);

		/*
		 * if all entries from ring buffer are being used then
		 * dequeue from ring buffer will fail.
		 */
		if (rc)
			count++;

		// sleep for 100usec to avoid busy looping
		usleep(100);

		if (count == 10) {
			REPLICA_ERRLOG("mempool(%s) is empty\n",
			    obj->ring->name);
			count = 0;
		}
	} while (rc != 0);

	if (obj->create)
		obj->create((char *)entry + obj->entry_offset, NULL, 0);

	return ((char *)entry + obj->entry_offset);
}

void
put_to_mempool(rte_smempool_t *obj, void *node)
{
	void *entry;
	entry = (void *) ((char *)node - obj->entry_offset);
	int rc;

	if (obj->free)
		obj->free(node, NULL);

	rc = rte_ring_enqueue(obj->ring, entry);
	if (rc) {
		REPLICA_ERRLOG("failed to put entry into mempool(%s)\n",
		    obj->ring->name);
	}
}

unsigned
get_num_entries_from_mempool(rte_smempool_t *obj)
{
	return (rte_ring_count(obj->ring));
}
