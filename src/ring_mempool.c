#include "ring_mempool.h"

int
init_mempool(rte_smempool_t *obj, size_t count, size_t mem_size,
    size_t offset, const char *ring_name, mempool_constructor_t *mem_init,
    mempool_destructor_t *mem_remove, mempool_reclaim_t *mem_reclaim)
{
	size_t i = 0;
	void *mem_entry = NULL;
	int rc = 0;

	obj->entry_offset = offset;
	obj->ring = rte_ring_create(ring_name, count, -1, RING_F_EXACT_SZ);

	for (i = 0; i < count; i++, mem_entry = NULL) {
		mem_entry = malloc(mem_size);
		if (!mem_entry) {
			printf("failed to get memory from mempool\n");
			rc = -1;
			break;
		}

		rc = rte_ring_enqueue(obj->ring, mem_entry);
		if(rc) {
			printf("failed to insert entry.. no buffer\n");
			rc = -1;
			break;
		}
	}

	obj->length = i;

	/*
	 * If we are unable to get provided number of entries in mempool
	 * then we will return
	 *	(-1) if we couldn't add single entry in mempool
	 *	(1)  if there are some entries in mempool that caller may use
	 */
	if (rc) {
		if (i)
			return 1;
		else
			return -1;
	}

	obj->create = mem_init;
	obj->free = mem_remove;
	obj->reclaim = mem_reclaim;

	return rc;
}


int
destroy_mempool(rte_smempool_t *obj)
{
	size_t i = 0;
	void *entry;
	int rc;

	if (rte_ring_count(obj->ring) != obj->length) {
		printf("there are still some orphan alloc from ring(%u)\n",
		    obj->length - rte_ring_count(obj->ring));
		return (obj->length - rte_ring_count(obj->ring));
	}

	for (i = 0; i < obj->length; i++) {
		rc = rte_ring_dequeue(obj->ring, &entry);
		if (rc)
			break;
		free(entry);
	}

	if (rte_ring_count(obj->ring) != 0) {
		printf("allocation happened while destroying mempool\n");
		return -1;
	}

	rte_ring_free(obj->ring);
	return 0;
}

void *
get_from_mempool(rte_smempool_t *obj)
{
	void *entry = NULL;
	int rc = 0;

	do {
		rc = rte_ring_dequeue(obj->ring, &entry);
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
		printf("failed to insert into ring\n");
	}

	return;
}
