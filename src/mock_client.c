#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>

#include "replication.h"
#include "istgt_integration.h"
#include "istgt_scsi.h"
#include "istgt_misc.h"
#include "istgt_proto.h"
#include "replication_misc.h"

typedef struct cargs_s {
	spec_t *spec;
	int workerid;
	pthread_mutex_t *mtx;
	pthread_cond_t *cv;
	int *count;
} cargs_t;


void create_mock_client(spec_t *);
void *reader(void *args);
void *mgmt_thrd(void *args);
void *writer(void *args);
void check_settings(spec_t *spec);
static void build_cmd(cargs_t *cargs, ISTGT_LU_CMD_Ptr lu_cmd, int opcode,
    int len);

cargs_t *all_cargs;
pthread_t *all_cthreads;

static void
build_cmd(cargs_t *cargs, ISTGT_LU_CMD_Ptr cmd, int opcode,
    int len)
{
	char *buf;
	int i;

	cmd->luworkerindx = cargs->workerid;
	if (opcode == 0) {
		buf = xmalloc(len);
		for (i = 0; i < len; i++)
			buf[i] = random() % 200;
		cmd->cdb0 = SBC_WRITE_16;
		cmd->iobuf[0].iov_base = buf;
		cmd->iobuf[0].iov_len = len;
		cmd->iobufsize = len;
	} else {
		cmd->cdb0 = SBC_READ_16;
		cmd->iobufsize = len;
	}
}

/*
 * Checks for settings on 'spec' to continue IOs
 */
void check_settings(spec_t *spec)
{
	while (spec->ready != true)
		sleep(1);

	return;
}

/*
 * Adds write IOs to replication module
 * Increments '*count' variable to set the completion of writer thread
 */
void *
writer(void *args)
{
	cargs_t *cargs = (cargs_t *)args;
	spec_t *spec = (spec_t *)cargs->spec;
	int blkcnt = spec->blockcnt;
	int blklen = spec->blocklen;
	int num_blocks = (blkcnt - 16);
	ISTGT_LU_CMD_Ptr lu_cmd;
	int rc, count = 0;
	uint64_t blk_offset, offset;
	int len_in_blocks, len;
	struct timespec now, start, prev;
	pthread_mutex_t *mtx = cargs->mtx;
	pthread_cond_t *cv = cargs->cv;
	int *cnt = cargs->count;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	snprintf(tinfo, 50, "mcwrite%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &prev);

	lu_cmd  = (ISTGT_LU_CMD_Ptr)malloc(sizeof (ISTGT_LU_CMD));
	memset(lu_cmd, 0, sizeof (ISTGT_LU_CMD));

	while (1) {
		check_settings(spec);

		blk_offset = random() % num_blocks;
		offset = blk_offset * blklen;
		len_in_blocks = random() % 15;
		len = len_in_blocks * blklen;

		build_cmd(cargs, lu_cmd, 0, len);

		while (spec->quiesce == 1)
			sleep(1);

		rc = replicate(spec, lu_cmd, offset, len);
		if (rc != len)
			goto end;
		count++;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 120)
			break;
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
			REPLICA_ERRLOG("wrote %d from %s\n", count, tinfo);
		}
	}
end:
	REPLICA_ERRLOG("exiting wrote %d from %s\n", count, tinfo);

	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return NULL;
}

/*
 * Adds read IOs to replication module
 * Increments '*count' variable to set the completion of reader thread
 */
void *
reader(void *args)
{
	cargs_t *cargs = (cargs_t *)args;
	spec_t *spec = (spec_t *)cargs->spec;
	int blkcnt = spec->blockcnt;
	int blklen = spec->blocklen;
	int num_blocks = (blkcnt - 16);
	ISTGT_LU_CMD_Ptr lu_cmd;
	int rc, count = 0;
	uint64_t blk_offset, offset;
	int len_in_blocks, len;
	struct timespec now, start, prev;
	pthread_mutex_t *mtx = cargs->mtx;
	pthread_cond_t *cv = cargs->cv;
	int *cnt = cargs->count;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	snprintf(tinfo, 50, "mcread%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &prev);

	lu_cmd  = malloc(sizeof (ISTGT_LU_CMD));
	memset(lu_cmd, 0, sizeof (ISTGT_LU_CMD));

	while (1) {
		check_settings(spec);

		blk_offset = random() % num_blocks;
		offset = blk_offset * blklen;
		len_in_blocks = random() & 15;
		len = len_in_blocks * blklen;

		build_cmd(cargs, lu_cmd, 1, len);

		rc = replicate(spec, lu_cmd, offset, len);
		if (rc != len)
			goto end;

		free(lu_cmd->data);
		lu_cmd->data = NULL;

		count++;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 10)
			break;
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
			REPLICA_ERRLOG("read %d from %s\n", count, tinfo);
		}
	}
end:
	REPLICA_ERRLOG("exiting read %d from %s\n", count, tinfo);

	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return NULL;
}

void *
mgmt_thrd(void *args)
{
	cargs_t *cargs = (cargs_t *)args;
	spec_t *spec = (spec_t *)cargs->spec;
	int count = 0;
	pthread_mutex_t *mtx = cargs->mtx;
	pthread_cond_t *cv = cargs->cv;
	int *cnt = cargs->count;
	struct timespec now, start, prev, p;
	char *snapname;
	int ret;

	snprintf(tinfo, 50, "mgmt%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	snapname = malloc(50);
	strcpy(snapname, "snap1");

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &prev);
	while (1) {
		clock_gettime(CLOCK_MONOTONIC, &p);
		ret = istgt_lu_create_snapshot(spec, snapname, random() % 2 + 2,
		    random() % 2 + 4);
		clock_gettime(CLOCK_MONOTONIC, &now);

		//REPLICA_LOG("snapshot response: %d time: %ld\n", ret, (now.tv_sec - p.tv_sec));
		sleep(1);
		count++;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 120)
			break;
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
			REPLICA_ERRLOG("sent %d from %s\n", count, tinfo);
		}
	}
	REPLICA_ERRLOG("exiting mgmt_thrd %s sent %d\n", tinfo, count);

	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return NULL;
}

/*
 * creates client threads that are needed to send read/write IOs to replication module.
 * cargs_t stores details that are sent to reader/writer threads
 * which isends IOs to replication module.
 */
void
create_mock_client(spec_t *spec)
{
	int num_threads = 6;
	int mgmt_threads = 1;
	int i;
	cargs_t *cargs;
	struct timespec now;
	pthread_mutex_t mtx;
	pthread_cond_t cv;
	int count;

	pthread_mutex_init(&mtx, NULL);
	pthread_cond_init(&cv, NULL);

	count = 0;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	all_cargs = (cargs_t *)malloc(sizeof (cargs_t) * (num_threads + mgmt_threads));
	all_cthreads = (pthread_t *)malloc(sizeof (pthread_t) * (num_threads + mgmt_threads));

	for (i = 0; i < num_threads; i++) {
		cargs = &(all_cargs[i]);
		cargs->workerid = i;
		cargs->spec = spec;
		cargs->mtx = &mtx;
		cargs->cv = &cv;
		cargs->count = &count;
		if (i < num_threads / 2)
			pthread_create(&all_cthreads[i], NULL, &writer, cargs);
		else
			pthread_create(&all_cthreads[i], NULL, &reader, cargs);
	}

	for (i = 0; i < mgmt_threads; i++) {
		cargs->workerid = num_threads + i;
		cargs = &(all_cargs[cargs->workerid]);
		cargs->spec = spec;
		cargs->mtx = &mtx;
		cargs->cv = &cv;
		cargs->count = &count;
		pthread_create(&all_cthreads[cargs->workerid], NULL, &mgmt_thrd, cargs);
	}

	MTX_LOCK(&mtx);
	while (count != (num_threads + mgmt_threads))
		pthread_cond_wait(&cv, &mtx);
	MTX_UNLOCK(&mtx);

	return;
}

