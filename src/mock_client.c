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
#include "assert.h"

typedef struct cargs_s {
	spec_t *spec;
	int workerid;
	pthread_mutex_t *mtx;
	pthread_cond_t *cv;
	int *count;
} cargs_t;

extern int ignore_io_error;

void create_mock_client(spec_t *, bool do_snap);
void *reader(void *args);
void *snapshot_thread(void *args);
void *writer(void *args);
void check_settings(spec_t *spec);
void wait_for_mock_clients(void);
static void build_cmd(cargs_t *cargs, ISTGT_LU_CMD_Ptr lu_cmd,
    SBC_OPCODE opcode, int len);

extern void init_snap_resp_list(void);
extern void destroy_snap_resp_list(void);
extern void update_snap_resp_list(spec_t *spec);
extern void verify_snap_response(int res);

cargs_t *all_cargs = NULL;
pthread_t *all_cthreads = NULL;

static void
build_cmd(cargs_t *cargs, ISTGT_LU_CMD_Ptr cmd, SBC_OPCODE opcode,
		int len)
{
	char *buf;
	int i;

	cmd->luworkerindx = cargs->workerid;
	switch (opcode) {
		case SBC_WRITE_16:
			buf = xmalloc(len);
			for (i = 0; i < len; i++)
				buf[i] = random() % 200;
			cmd->cdb0 = SBC_WRITE_16;
			cmd->iobuf[0].iov_base = buf;
			cmd->iobuf[0].iov_len = len;
			cmd->iobufsize = len;
			break;

		case SBC_READ_16:
			cmd->cdb0 = SBC_READ_16;
			cmd->iobufsize = len;
			break;

		case SBC_SYNCHRONIZE_CACHE_16:
			cmd->cdb0 = SBC_SYNCHRONIZE_CACHE_16;
			cmd->iobufsize = 0;
			break;

		default:
			break;
	}
}

/*
 * Checks for settings on 'spec' to continue IOs
 */
void
check_settings(spec_t *spec)
{
	while (spec->ready != true)
		sleep(1);
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
	SBC_OPCODE opcode;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	snprintf(tinfo, 50, "mcwrite%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &prev);

	lu_cmd  = (ISTGT_LU_CMD_Ptr)malloc(sizeof (ISTGT_LU_CMD));
	memset(lu_cmd, 0, sizeof (ISTGT_LU_CMD));

	check_settings(spec);

	while (1) {

		opcode = SBC_WRITE_16;
		blk_offset = random() % num_blocks;
		offset = blk_offset * blklen;
		len_in_blocks = random() % 15;
		len = len_in_blocks * blklen;

		if ((random() % 3) == 0) {
			opcode = SBC_SYNCHRONIZE_CACHE_16;
			len = 0;
		}

		build_cmd(cargs, lu_cmd, opcode, len);

		rc = replicate(spec, lu_cmd, offset, len);

		if (rc != len && !ignore_io_error)
			goto end;

		count++;

		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 120)
			break;
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
//			REPLICA_ERRLOG("wrote %d from %s\n", count, tinfo);
		}
	}
end:
	REPLICA_ERRLOG("exiting wrote %d from %s\n", count, tinfo);

	free(lu_cmd);

	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return (NULL);
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
	SBC_OPCODE opcode = SBC_READ_16;

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	snprintf(tinfo, 50, "mcread%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	clock_gettime(CLOCK_MONOTONIC, &start);
	clock_gettime(CLOCK_MONOTONIC, &prev);

	lu_cmd  = malloc(sizeof (ISTGT_LU_CMD));
	memset(lu_cmd, 0, sizeof (ISTGT_LU_CMD));

	check_settings(spec);

	while (1) {

		blk_offset = random() % num_blocks;
		offset = blk_offset * blklen;
		len_in_blocks = random() & 15;
		len = len_in_blocks * blklen;

		build_cmd(cargs, lu_cmd, opcode, len);

		rc = replicate(spec, lu_cmd, offset, len);

		if (rc != len && !ignore_io_error)
			goto end;

		if (lu_cmd->data)
			xfree(lu_cmd->data);

		lu_cmd->data = NULL;

		count++;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 10)
			break;
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
//			REPLICA_ERRLOG("read %d from %s\n", count, tinfo);
		}
	}
end:
	REPLICA_ERRLOG("exiting read %d from %s\n", count, tinfo);

	if (lu_cmd->data)
		xfree(lu_cmd->data);
	free(lu_cmd);

	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return (NULL);
}

void *
snapshot_thread(void *args)
{
	cargs_t *cargs = (cargs_t *)args;
	spec_t *spec = (spec_t *)cargs->spec;
	int count = 0;
	pthread_mutex_t *mtx = cargs->mtx;
	pthread_cond_t *cv = cargs->cv;
	int *cnt = cargs->count;
	struct timespec now, start, cmd_start, cmd_time;
	char *snapname;
	int ret;
	int io_wait_time, wait_time;

	snprintf(tinfo, 50, "clientmgmt%d", cargs->workerid);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	snapname = malloc(50);
	strcpy(snapname, "snap1");

	init_snap_resp_list();

	clock_gettime(CLOCK_MONOTONIC, &now);
	srandom(now.tv_sec);

	clock_gettime(CLOCK_MONOTONIC, &start);
	while (1) {
		update_snap_resp_list(spec);
		io_wait_time = random() % 2 + 2;
		wait_time = random() % 2 + 4;

		clock_gettime(CLOCK_MONOTONIC, &cmd_start);
		ret = istgt_lu_create_snapshot(spec, snapname, io_wait_time,
		    wait_time);
		timesdiff(CLOCK_MONOTONIC, cmd_start, now, cmd_time);

		VERIFY(cmd_time.tv_sec <= (wait_time + 1));

		verify_snap_response(ret);

		sleep(1);
		count++;
		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - start.tv_sec > 120)
			break;
	}
	REPLICA_ERRLOG("exiting snapshot thread %s sent %d\n", tinfo, count);

	destroy_snap_resp_list();
	free(snapname);
	MTX_LOCK(mtx);
	*cnt = *cnt + 1;
	pthread_cond_signal(cv);
	MTX_UNLOCK(mtx);

	return (NULL);
}

/*
 * creates client threads that are needed to send read/write IOs to replication
 * module.  cargs_t stores details that are sent to reader/writer threads which
 * isends IOs to replication module.
 */

void
create_mock_client(spec_t *spec, bool do_snap)
{
	int num_threads = 6;
	int snap_thread = (do_snap) ? 1 : 0;
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

	all_cargs = (cargs_t *)malloc(sizeof (cargs_t) *
	    (num_threads + snap_thread));
	all_cthreads = (pthread_t *)malloc(sizeof (pthread_t) *
	    (num_threads + snap_thread));

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

	if (do_snap) {
		cargs = &(all_cargs[num_threads]);
		cargs->workerid = num_threads;
		cargs->spec = spec;
		cargs->mtx = &mtx;
		cargs->cv = &cv;
		cargs->count = &count;
		pthread_create( &all_cthreads[cargs->workerid], NULL, &snapshot_thread,
		    cargs);
	}

	MTX_LOCK(&mtx);
	while (count != (num_threads + snap_thread))
		pthread_cond_wait(&cv, &mtx);
	MTX_UNLOCK(&mtx);

	free(all_cargs);
	free(all_cthreads);
	all_cthreads = NULL;
	all_cargs = NULL;
}

void
wait_for_mock_clients()
{
	int count = 10;
	while (count) {
		__sync_synchronize();
		usleep(100000);
		if (!all_cthreads)
			return;
		count--;
	}

	REPLICA_ERRLOG("mock client is still running\n");
	abort();
}
