#include <stdio.h>
#include <string.h>
#include "ring_mempool.h"

typedef struct test_node {
	int a;
	int b;
	int c;
} test_node_t;

struct test_node_entry {
	int garbage;
	test_node_t node;
};

void check_mempool_size(rte_smempool_t *mempool);
void verify_mempool_values_n_destroy(rte_smempool_t *mempool);
__thread char  tinfo[50] =  {0};

/*
 * Mempool size must be equal to mempool->length.
 * We will traverse through mempool to verify pool size.
 */
void
check_mempool_size(rte_smempool_t *mempool)
{
	unsigned i = 0;
	test_node_t **node = NULL;
	test_node_t *temp;

	node = (test_node_t **) malloc(sizeof (*node) * mempool->length);

	for (i = 0; i < mempool->length; i++, temp = NULL) {
		temp = get_from_mempool(mempool);
		temp->c = i*3;
		temp->a = i;
		temp->b = i*2;
		node[i] = temp;
	}

	for (i = 0; i < mempool->length; i++) {
		put_to_mempool(mempool, node[i]);
	}

	free(node);
}

/*
 * Fetch all entries from mempool and verify stored value in all entries.
 * Also perform delete operation to check if mempool get destroyed or not.
 */
void
verify_mempool_values_n_destroy(rte_smempool_t *mempool)
{
	unsigned i = 0;
	test_node_t **node = NULL;
	test_node_t *temp;
	int rc = 0;

	node = (test_node_t **) malloc(sizeof (*node) * mempool->length);

	for (i = 0; i < mempool->length; i++, temp = NULL) {
		temp = get_from_mempool(mempool);
		if ((temp->c != (temp->a * 3)) ||
			((temp->b != (temp->a *2)))) {
			printf("error happened in verification\n");
			exit(1);
		}
		node[i] = temp;
	}

	rc = destroy_mempool(mempool);
	if (rc <= 0) {
		printf("failed to verify destroy\n");
		exit(2);
	}

	for (i = 0; i < mempool->length; i++) {
		put_to_mempool(mempool, node[i]);
	}

	free(node);
}

int
main(void)
{
	rte_smempool_t mempool;

	memset(&mempool, 0, sizeof (mempool));

	if (init_mempool(&mempool, 100, sizeof (struct test_node_entry),
	    offsetof(struct test_node_entry, node), "test_mempool",
	    NULL, NULL, NULL, true)) {
		destroy_mempool(&mempool);
		return (0);
	}

	check_mempool_size(&mempool);
	verify_mempool_values_n_destroy(&mempool);

	destroy_mempool(&mempool);
	return (0);
}
