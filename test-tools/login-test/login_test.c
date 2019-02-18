#include <iscsi.h>
#include <scsi-lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

void usage()
{
	printf("login_login -h <target, target:port> -t <sec> (default=5)\n");
	printf("-v for verbose output\n");
}

int main(int argc, char **argv)
{
	int opt;
	int verbose = 0;
	int err = 0;
	int iter_count = 0;
	int timeout = 5;
	const char *target = NULL;

	struct iscsi_context *iscsi_ctx;
	struct iscsi_url *iscsi_url;

	if (argc < 2) {
		usage();
	}

	while ((opt = getopt(argc, argv, "vt:h:")) != -1) {
		switch (opt) {
		case 'v':
			verbose = 9;
			break;
		case 'h':
			target = optarg;
			break;
		case 't':
			timeout = atoi(optarg);
			break;
		default:
			printf("unknow option %s\n", optarg);
			usage();
			return 1;
		}
	}

	if (target == NULL) {
		usage();
		return 1;
	}


	printf("Running test against: %s with timeout set to %d seconds\n",
	       target, timeout);

	while (iter_count < 400) {
		if ((iscsi_ctx =
			     iscsi_create_context("iqn.2019-2.io.openebs:test"))
		    == NULL) {
			printf("failed to create iscsi context\n");
			return 1;
		}

		iscsi_url = iscsi_parse_portal_url(iscsi_ctx, target);
		if (verbose)
			printf("target: %s, portal: %s, lu %d\n",
			       iscsi_url->target, iscsi_url->portal,
			       iscsi_url->lun);

		iscsi_set_session_type(iscsi_ctx, ISCSI_SESSION_DISCOVERY);
		iscsi_set_header_digest(iscsi_ctx, ISCSI_HEADER_DIGEST_NONE);
		iscsi_set_log_level(iscsi_ctx, verbose);
		iscsi_set_log_fn(iscsi_ctx, iscsi_log_to_stderr);
		iscsi_set_timeout(iscsi_ctx, timeout);

		if ((err = iscsi_connect_sync(iscsi_ctx, iscsi_url->portal))
		    != 0) {
			printf("failed to connect %s\n", target);
			return 1;
		}

		if ((err = iscsi_login_sync(iscsi_ctx)) != 0) {
			printf("failed to login %s\n",
			       iscsi_get_error(iscsi_ctx));
			return 1;
		}

		struct iscsi_discovery_address *addr =
			iscsi_discovery_sync(iscsi_ctx);

		if (addr == NULL) {
			printf("discovery failed: %s\n",
			       iscsi_get_error(iscsi_ctx));
			return 1;
		}

		struct iscsi_discovery_address *curr;
		int target_count = 0;
		int portal_count = 0;

		do {
			if (verbose)
				printf("target name: %s\n", addr->target_name);
			target_count++;
			struct iscsi_target_portal *cp;
			do {
				if (verbose)
					printf("target portal %s\n",
					       addr->portals->portal);
				portal_count++;
			} while ((cp = addr->portals->next) != NULL);
		} while ((curr = addr->next) != NULL);

		if (portal_count == 0 || target_count == 0) {
			printf("failed to find any target(s) test failed..\n");
			return 1;
		}

		assert(iscsi_logout_sync(iscsi_ctx) == 0);

		iscsi_destroy_url(iscsi_url);
		iscsi_destroy_context(iscsi_ctx);
		if (verbose)
			printf("-- iteration count: %d\n", iter_count);

		iter_count++;
		// make sure we give a sign of life...
		if ((iter_count % 100 == 0) && verbose == 0)
			printf("%d  discoveries succesfull\n", iter_count);
	}
	return 0;
}
