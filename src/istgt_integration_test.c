#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "istgt_misc.h"
#include "istgt_proto.h"
#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"

__thread char tinfo[50] = {0};
int g_trace_flag = 0;
typedef struct rebuild_test_s {
	pthread_cond_t test_state_cv;
	pthread_mutex_t test_mtx;
	spec_t *spec;
	bool replica_killing;
	bool reregister_replica_test;
} rebuild_test_t;

typedef struct rargs_s {
	/* IP:Port on which replica is listening */
	char replica_ip[MAX_IP_LEN];
	uint16_t replica_port;

	/* IP:Port on which controller is listening */
	char ctrl_ip[MAX_IP_LEN];
	uint16_t ctrl_port;

	/* fd for management connection from replica to tgt */
	int mgmtfd;

	/* fd for data connection from tgt to replica */
	int iofd;

	/* fd of sparse file to write data for replica */
	int file_fd;

	/* mtx to change mgmt_send_list */
	pthread_mutex_t mgmt_send_mtx;

	/* mtx to change mgmt_recv_list */
	pthread_mutex_t mgmt_recv_mtx;

	pthread_cond_t mgmt_send_cv;
	pthread_cond_t mgmt_recv_cv;

	TAILQ_HEAD(, zvol_io_cmd_s) mgmt_recv_list;
	TAILQ_HEAD(, zvol_io_cmd_s) mgmt_send_list;

	/* mtx to change io_send_list */
	pthread_mutex_t io_send_mtx;

	/* mtx to change io_recv_list */
	pthread_mutex_t io_recv_mtx;

	pthread_cond_t io_send_cv;
	pthread_cond_t io_recv_cv;

	TAILQ_HEAD(, zvol_io_cmd_s) io_recv_list;
	TAILQ_HEAD(, zvol_io_cmd_s) io_send_list;

	char volname[MAX_NAME_LEN];
	char file_path[MAX_NAME_LEN];

	zvol_status_t 	zrepl_status;
	zvol_rebuild_status_t zrepl_rebuild_status;
	
	int rebuild_status_enquiry;
	/* flag to stop replica threads once this is set to 1 */
	int kill_replica;
	int kill_is_over;
} rargs_t;


typedef struct zvol_io_cmd_s {
	TAILQ_ENTRY(zvol_io_cmd_s) next;
	zvol_io_hdr_t 	hdr;
	void		*buf;
} zvol_io_cmd_t;

/*
 * Allocate zio command along with
 * buffer needed for IO completion.
 */
static zvol_io_cmd_t *
zio_cmd_alloc(zvol_io_hdr_t *hdr)
{
	zvol_io_cmd_t *zio_cmd = malloc(
	    sizeof (zvol_io_cmd_t));

	bcopy(hdr, &zio_cmd->hdr, sizeof (zio_cmd->hdr));
	if ((hdr->opcode == ZVOL_OPCODE_WRITE) ||
	    (hdr->opcode == ZVOL_OPCODE_HANDSHAKE) ||
	    (hdr->opcode == ZVOL_OPCODE_REPLICA_STATUS) ||
	    (hdr->opcode == ZVOL_OPCODE_OPEN) ||
	    (hdr->opcode == ZVOL_OPCODE_START_REBUILD) ||
	    (hdr->opcode == ZVOL_OPCODE_PREPARE_FOR_REBUILD)) {
		zio_cmd->buf = malloc(sizeof (char) * hdr->len);
	} else
		zio_cmd->buf = NULL;

	return (zio_cmd);
}

/*
 * Free zio command along with buffer.
 */
static void
zio_cmd_free(zvol_io_cmd_t **cmd)
{
	zvol_io_cmd_t *zio_cmd = *cmd;
	if (zio_cmd->buf != NULL)
		free(zio_cmd->buf);
	zio_cmd->buf = NULL;
	free(zio_cmd);
	*cmd = NULL;
}

/*
 * blocks to read from the wire
 */
static int
uzfs_zvol_socket_read(int fd, char *buf, uint64_t nbytes)
{
	ssize_t count = 0;
	char *p = buf;
	while (nbytes) {
		count = read(fd, (void *)p, nbytes);
		if (count <= 0) {
			REPLICA_ERRLOG("Read error:%d\n", errno);
			return (-1);
		}
		p += count;
		nbytes -= count;
	}
	return (0);
}

extern cstor_conn_ops_t cstor_ops;

rargs_t *all_rargs;
pthread_t *all_rthrds;

/*
 * Handles ZVOL_OPCODE_OPEN mgmt command
 */
static void
handle_open(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);
	zvol_op_open_data_t *data = zio_cmd->buf;

	REPLICA_LOG("%d %s %d %d %d %s\n", rargs->replica_port, rargs->file_path, rargs->file_fd,
	    data->timeout, data->tgt_block_size, data->volname);
	free(zio_cmd->buf);
	hdr->len = 0;
	zio_cmd->buf = NULL;
	hdr->status = ZVOL_OP_STATUS_OK;
}

/*
 * Handles ZVOL_OPCODE_REPLICA_STATUS mgmt command
 */
static void
handle_replica_start_rebuild(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);

	/* Mark rebuild is in progress */
	rargs->zrepl_rebuild_status = ZVOL_REBUILDING_IN_PROGRESS;

	if (zio_cmd->buf)
		free(zio_cmd->buf);
	hdr->len = 0;
	zio_cmd->buf = NULL;
	hdr->status = ZVOL_OP_STATUS_OK;
}

/*
 * Handles ZVOL_OPCODE_REPLICA_STATUS mgmt command
 */
static void
handle_replica_status(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);
	zrepl_status_ack_t *zrepl_status;

	zrepl_status = malloc(sizeof (*zrepl_status));
	/* After 2 enquiries, mark replica healthy */	
	if ((rargs->zrepl_status != ZVOL_STATUS_HEALTHY) &&
	    (rargs->rebuild_status_enquiry >= 2)) {
		rargs->zrepl_status = ZVOL_STATUS_HEALTHY;
		rargs->zrepl_rebuild_status = ZVOL_REBUILDING_DONE;
		rargs->rebuild_status_enquiry = 0;
	}

	if (rargs->zrepl_rebuild_status == ZVOL_REBUILDING_IN_PROGRESS)
		rargs->rebuild_status_enquiry++;
	zrepl_status->state = rargs->zrepl_status;
	zrepl_status->rebuild_status = rargs->zrepl_rebuild_status;

	if (zio_cmd->buf)
		free(zio_cmd->buf);
	hdr->len = sizeof (*zrepl_status);
	zio_cmd->buf = zrepl_status;
	hdr->status = ZVOL_OP_STATUS_OK;
}

/*
 * Handles ZVOL_OPCODE_HANDSHAKE mgmt command
 */
static void
handle_handshake(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);

	if (strcmp(zio_cmd->buf, rargs->volname) != 0)
		REPLICA_ERRLOG("volname not matching %s %s\n",
		    (char *)zio_cmd->buf, rargs->volname);

	mgmt_ack_t *mgmt_ack = malloc(sizeof (mgmt_ack_t));
	memset(mgmt_ack, 0, sizeof (mgmt_ack_t));
	mgmt_ack->pool_guid = 5000;
	mgmt_ack->zvol_guid = 1000;
	mgmt_ack->port = rargs->replica_port;
	strncpy(mgmt_ack->ip, rargs->replica_ip, sizeof (mgmt_ack->ip));
	strncpy(mgmt_ack->volname, rargs->volname, sizeof (mgmt_ack->volname));
	hdr->status = ZVOL_OP_STATUS_OK;
	hdr->checkpointed_io_seq = 10000;
	hdr->len = sizeof (mgmt_ack_t);

	if (zio_cmd->buf != NULL)
		free(zio_cmd->buf);

	zio_cmd->buf = mgmt_ack;
}

/*
 * blocks to write on the wire
 */
static int
uzfs_zvol_socket_write(int fd, char *buf, uint64_t nbytes)
{
	ssize_t count = 0;
	char *p = buf;
	while (nbytes) {
		count = write(fd, (void *)p, nbytes);
		if (count <= 0) {
			REPLICA_ERRLOG("Write error:%d\n", errno);
			return (-1);
		}
		p += count;
		nbytes -= count;
	}
	return (0);
}

/*
 * This thread takes mgmt commands from mgmt_send_list and writes to mgmtfd
 */
static void *
mock_repl_mgmt_sender(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	zvol_io_cmd_t *zio_cmd;
	int rc;

	snprintf(tinfo, 50, "mocksend%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	while (1) {
		MTX_LOCK(&rargs->mgmt_send_mtx);
		while (TAILQ_EMPTY(&rargs->mgmt_send_list)) {
			pthread_cond_wait(&rargs->mgmt_send_cv, &rargs->mgmt_send_mtx);
			if (rargs->kill_replica == true) {
				MTX_UNLOCK(&rargs->mgmt_send_mtx);
				goto end;
			}
		}
		zio_cmd = TAILQ_FIRST(&rargs->mgmt_send_list);
		TAILQ_REMOVE(&rargs->mgmt_send_list, zio_cmd, next);
		MTX_UNLOCK(&rargs->mgmt_send_mtx);

		rc = uzfs_zvol_socket_write(rargs->mgmtfd, (char *)&zio_cmd->hdr, sizeof (zio_cmd->hdr));
		if (rc != 0)
			goto end;
		if (zio_cmd->buf != NULL) {
			rc = uzfs_zvol_socket_write(rargs->mgmtfd, zio_cmd->buf, zio_cmd->hdr.len);
			if (rc != 0)
				goto end;
		}
		zio_cmd_free(&zio_cmd);
	}
end:
	REPLICA_LOG("mock_repl_mgmt_sender exiting....\n");
	return NULL;
}

/*
 * This thread takes IOs from io_send_list and writes to iofd
 */
static void *
mock_repl_io_sender(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	zvol_io_cmd_t *zio_cmd;
	int rc;


	snprintf(tinfo, 50, "mockiosend%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	while (1) {
		MTX_LOCK(&rargs->io_send_mtx);
		while (TAILQ_EMPTY(&rargs->io_send_list)) {
			pthread_cond_wait(&rargs->io_send_cv, &rargs->io_send_mtx);
			if (rargs->kill_replica == true) {
				MTX_UNLOCK(&rargs->io_send_mtx);
				goto end;
			}
		}
		zio_cmd = TAILQ_FIRST(&rargs->io_send_list);
		TAILQ_REMOVE(&rargs->io_send_list, zio_cmd, next);
		MTX_UNLOCK(&rargs->io_send_mtx);

		rc = uzfs_zvol_socket_write(rargs->iofd, (char *)&zio_cmd->hdr, sizeof (zio_cmd->hdr));
		if (rc != 0)
			goto end;
		if (zio_cmd->buf != NULL) {
			rc = uzfs_zvol_socket_write(rargs->iofd, zio_cmd->buf, zio_cmd->hdr.len);
			if (rc != 0)
				goto end;
		}
		zio_cmd_free(&zio_cmd);
	}
end:
	REPLICA_LOG("mock_repl_io_sender exiting....\n");
	return NULL;
}

static void
handle_read(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);
	uint64_t offset = hdr->offset;
	uint64_t len = hdr->len, nbytes;
	uint8_t *orig_data, *data;
	struct zvol_io_rw_hdr *io_rw_hdr;
	int rc;

	orig_data = data = malloc(len + sizeof (struct zvol_io_rw_hdr));
	nbytes = 0;
	data += sizeof (struct zvol_io_rw_hdr);
	while((rc = pread(rargs->file_fd, data + nbytes, len - nbytes, offset + nbytes))) {
		if(rc == -1) {
			if(errno == EAGAIN) {
				sleep(1);
				continue;
			}
			REPLICA_ERRLOG("pread failed, errorno:%d", errno);
			exit(EXIT_FAILURE);
		}
		nbytes += rc;
		if(nbytes == hdr->len) {
			break;
		}
	}
	io_rw_hdr = (struct zvol_io_rw_hdr *)orig_data;
	io_rw_hdr->io_num = 2000;
	io_rw_hdr->len = hdr->len;
	hdr->status = ZVOL_OP_STATUS_OK;
	hdr->len = len + sizeof (struct zvol_io_rw_hdr);
	zio_cmd->buf = orig_data;
}

static void
handle_write(rargs_t *rargs, zvol_io_cmd_t *zio_cmd)
{
	int rc;
	uint64_t nbytes = 0;
	zvol_io_hdr_t *hdr = &(zio_cmd->hdr);
	uint8_t *data = zio_cmd->buf;
	struct zvol_io_rw_hdr *io_rw_hdr = (struct zvol_io_rw_hdr *)data;

	data += sizeof(struct zvol_io_rw_hdr);
	while((rc = pwrite(rargs->file_fd, data + nbytes, io_rw_hdr->len - nbytes, hdr->offset + nbytes))) {
		if(rc == -1 ) {
			if(errno == EAGAIN) {
				sleep(1);
				continue;
			}
			REPLICA_ERRLOG("pwrite failed, errorno:%d", errno);
			exit(EXIT_FAILURE);
		}
		nbytes += rc;
		if(nbytes == io_rw_hdr->len) {
			break;
		}
	}
	hdr->status = ZVOL_OP_STATUS_OK;
	free(zio_cmd->buf);
	zio_cmd->buf = NULL;
}

/*
 * This thread takes IOs from io_recv_list, executes them, and,
 * adds responses to io_send_list
 */
static void *
mock_repl_io_worker(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	zvol_io_cmd_t *zio_cmd;
	zvol_io_hdr_t *hdr;
	int read_count = 0, write_count = 0;
	struct timespec now, prev;

	snprintf(tinfo, 50, "mockiowork%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	clock_gettime(CLOCK_MONOTONIC, &prev);
	while (1) {
		MTX_LOCK(&rargs->io_recv_mtx);
		while (TAILQ_EMPTY(&(rargs->io_recv_list))) {
			pthread_cond_wait(&rargs->io_recv_cv, &rargs->io_recv_mtx);
			if (rargs->kill_replica == true) {
				MTX_UNLOCK(&rargs->io_recv_mtx);
				goto end;
			}
		}
		zio_cmd = TAILQ_FIRST(&rargs->io_recv_list);
		TAILQ_REMOVE(&rargs->io_recv_list, zio_cmd, next);
		MTX_UNLOCK(&rargs->io_recv_mtx);
		hdr = &zio_cmd->hdr;
		switch(hdr->opcode)
		{
			case ZVOL_OPCODE_OPEN:
				handle_open(rargs, zio_cmd);
				break;
			case ZVOL_OPCODE_READ:
				handle_read(rargs, zio_cmd);
				read_count++;
				break;
			case ZVOL_OPCODE_WRITE:
				handle_write(rargs, zio_cmd);
				write_count++;
				break;
			default:
				break;
		}

		clock_gettime(CLOCK_MONOTONIC, &now);
		if (now.tv_sec - prev.tv_sec > 1) {
			prev = now;
			REPLICA_ERRLOG("read %d wrote %d from %s\n", read_count, write_count, tinfo);
		}
		MTX_LOCK(&rargs->io_send_mtx);
		TAILQ_INSERT_TAIL(&rargs->io_send_list, zio_cmd, next);
		pthread_cond_signal(&rargs->io_send_cv);
		MTX_UNLOCK(&rargs->io_send_mtx);
	}
end:
	REPLICA_LOG("mock_repl_io_worker exiting....\n");
	return NULL;
}

/*
 * This thread takes mgmt commands from mgmt_recv_list
 * executes them and adds to mgmt_send_list
 */
static void *
mock_repl_mgmt_worker(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	zvol_io_cmd_t *zio_cmd;
	zvol_io_hdr_t *hdr;


	snprintf(tinfo, 50, "mockwork%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	while (1) {
		MTX_LOCK(&rargs->mgmt_recv_mtx);
		while (TAILQ_EMPTY(&(rargs->mgmt_recv_list))) {
			pthread_cond_wait(&rargs->mgmt_recv_cv, &rargs->mgmt_recv_mtx);
			if (rargs->kill_replica == true) {
				MTX_UNLOCK(&rargs->mgmt_recv_mtx);
				goto end;
			}
		}
		zio_cmd = TAILQ_FIRST(&rargs->mgmt_recv_list);
		TAILQ_REMOVE(&rargs->mgmt_recv_list, zio_cmd, next);
		MTX_UNLOCK(&rargs->mgmt_recv_mtx);
		hdr = &zio_cmd->hdr;
		switch(hdr->opcode)
		{
			case ZVOL_OPCODE_HANDSHAKE:
			case ZVOL_OPCODE_PREPARE_FOR_REBUILD:
				handle_handshake(rargs, zio_cmd);
				break;
			case ZVOL_OPCODE_REPLICA_STATUS:
				handle_replica_status(rargs, zio_cmd);
				break;
			case ZVOL_OPCODE_START_REBUILD:
				handle_replica_start_rebuild(rargs, zio_cmd);
				break;
			default:
				goto end;
		}

		MTX_LOCK(&rargs->mgmt_send_mtx);
		TAILQ_INSERT_TAIL(&rargs->mgmt_send_list, zio_cmd, next);
		pthread_cond_signal(&rargs->mgmt_send_cv);
		MTX_UNLOCK(&rargs->mgmt_send_mtx);
	}
end:
	REPLICA_LOG("mock_repl_mgmt_worker exiting....\n");
	return NULL;
}

/*
 * This thread reads mgmt commands from mgmtfd and adds to mgmt_recv_list
 */
static void *
mock_repl_mgmt_receiver(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	int rc;
	zvol_io_cmd_t *zio_cmd;
	zvol_io_hdr_t *hdr = malloc(sizeof (zvol_io_hdr_t));
	memset(hdr, 0, sizeof (zvol_io_hdr_t));

	snprintf(tinfo, 50, "mockmgmt%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	while (1) {
		rc = uzfs_zvol_socket_read(rargs->mgmtfd, (char *)hdr,
		    sizeof (*hdr));
		if (rc != 0) {
			REPLICA_ERRLOG("error reading from socket: %d\n", errno);
			goto end;
		}

		zio_cmd = zio_cmd_alloc(hdr);
		/* Read payload for commands which have it */
		if (hdr->len != 0) {
			rc = uzfs_zvol_socket_read(rargs->mgmtfd, zio_cmd->buf, hdr->len);
			if (rc != 0) {
				zio_cmd_free(&zio_cmd);
				REPLICA_ERRLOG("Socket read failed with "
				    "error: %d\n", errno);
				goto end;
			}
		} else {
			REPLICA_ERRLOG("Unexpected payload for opcode %d\n",
			    hdr->opcode);
			zio_cmd_free(&zio_cmd);
			goto end;
		}

		MTX_LOCK(&rargs->mgmt_recv_mtx);
		TAILQ_INSERT_TAIL(&rargs->mgmt_recv_list, zio_cmd, next);
		pthread_cond_signal(&rargs->mgmt_recv_cv);
		MTX_UNLOCK(&rargs->mgmt_recv_mtx);
	}
end:
	free(hdr);
	REPLICA_LOG("mock_repl_mgmt_receiver exiting....\n");
	return NULL;
}

/*
 * This thread reads IOs from iofd and adds to io_recv_list
 */
static void *
mock_repl_io_receiver(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	int rc;
	zvol_io_cmd_t *zio_cmd;
	zvol_io_hdr_t *hdr = malloc(sizeof (zvol_io_hdr_t));
	memset(hdr, 0, sizeof (zvol_io_hdr_t));

	snprintf(tinfo, 50, "mockiorecv%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	while (1) {
		rc = uzfs_zvol_socket_read(rargs->iofd, (char *)hdr,
		    sizeof (*hdr));
		if (rc != 0) {
			REPLICA_ERRLOG("error reading from socket: %d\n", errno);
			goto end;
		}

		zio_cmd = zio_cmd_alloc(hdr);
		/* Read payload for commands which have it */
		if (zio_cmd->buf != NULL) {
			rc = uzfs_zvol_socket_read(rargs->iofd, zio_cmd->buf, hdr->len);
			if (rc != 0) {
				zio_cmd_free(&zio_cmd);
				REPLICA_ERRLOG("Socket read failed with "
				    "error: %d\n", errno);
				goto end;
			}
		}

		MTX_LOCK(&rargs->io_recv_mtx);
		TAILQ_INSERT_TAIL(&rargs->io_recv_list, zio_cmd, next);
		pthread_cond_signal(&rargs->io_recv_cv);
		MTX_UNLOCK(&rargs->io_recv_mtx);
	}
end:
	free(hdr);
	REPLICA_LOG("mock_repl_io_receiver exiting....\n");
	return NULL;
}

pthread_mutexattr_t mutex_attr;
extern void create_mock_client(spec_t *);
uint64_t blocklen;
uint64_t volsize;
char *vol_name;
int total_time_in_sec;
int test_id;

/*
 * Initialize mutex, cv variables in spec which are required for repliation
 */
static int
initialize_spec(spec_t *spec)
{
	int k, rc;
	memset(spec, 0, sizeof (spec_t));
	spec->volname = xstrdup("vol1");
	spec->blocklen = blocklen;
	spec->blockcnt = (volsize / spec->blocklen);

	for(k = 0; k < ISTGT_MAX_NUM_LUWORKERS; k++) {
		rc = pthread_cond_init(&spec->luworker_rcond[k], NULL);
		if (rc != 0) {
			REPLICA_ERRLOG("luworker %d rcond_init() failed errno:%d\n", k, errno);
			return -1;
		}

		rc = pthread_mutex_init(&spec->luworker_rmutex[k], &mutex_attr);
		if (rc != 0) {
			REPLICA_ERRLOG("luworker %d mutex_init() failed errno:%d\n", k, errno);
			return -1;
		}
	}
	return 0;
}

/*
 * main replica thread to accept for data connections
 * creates other worker threads for reading/writing/executing
 */
static void *
mock_repl(void *args)
{
	rargs_t *rargs = (rargs_t *)args;
	int file_fd, sfd, mgmtfd;
	pthread_t mgmt_receiver, mgmt_sender, mgmt_worker;
	pthread_t io_receiver, io_sender, io_worker1, io_worker2, io_worker3;
	struct sockaddr saddr;
	socklen_t slen;
	zvol_io_cmd_t *zio_cmd;

	snprintf(tinfo, 50, "mock%d", rargs->replica_port);
	prctl(PR_SET_NAME, tinfo, 0, 0, 0);

	rargs->file_fd = file_fd = open(rargs->file_path, O_RDWR, 0666);

	//Create listener for io connections from controller and add to epoll
	if((sfd = cstor_ops.conn_listen(rargs->replica_ip, rargs->replica_port, 32, 0)) < 0) {
		REPLICA_ERRLOG("conn_listen() failed, errorno:%d", errno);
		exit(EXIT_FAILURE);
        }

	//Connect to controller to start handshake and connect to epoll
	while((rargs->mgmtfd = mgmtfd = cstor_ops.conn_connect(rargs->ctrl_ip, rargs->ctrl_port)) < 0) {
		REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
		sleep(1);
	}

	pthread_mutex_init(&rargs->mgmt_recv_mtx, NULL);
	pthread_mutex_init(&rargs->mgmt_send_mtx, NULL);

	pthread_cond_init(&rargs->mgmt_recv_cv, NULL);
	pthread_cond_init(&rargs->mgmt_send_cv, NULL);

	TAILQ_INIT(&rargs->mgmt_recv_list);
	TAILQ_INIT(&rargs->mgmt_send_list);

	pthread_mutex_init(&rargs->io_recv_mtx, NULL);
	pthread_mutex_init(&rargs->io_send_mtx, NULL);

	pthread_cond_init(&rargs->io_recv_cv, NULL);
	pthread_cond_init(&rargs->io_send_cv, NULL);

	TAILQ_INIT(&rargs->io_recv_list);
	TAILQ_INIT(&rargs->io_send_list);

	rargs->zrepl_status = ZVOL_STATUS_DEGRADED; 
	rargs->zrepl_rebuild_status = ZVOL_REBUILDING_INIT;
	rargs->rebuild_status_enquiry = 0;

	pthread_create(&mgmt_receiver, NULL, &mock_repl_mgmt_receiver, args);
	pthread_create(&mgmt_sender, NULL, &mock_repl_mgmt_sender, args);
	pthread_create(&mgmt_worker, NULL, &mock_repl_mgmt_worker, args);


	rargs->iofd = accept(sfd, &saddr, &slen);
	pthread_create(&io_receiver, NULL, &mock_repl_io_receiver, args);
	pthread_create(&io_sender, NULL, &mock_repl_io_sender, args);
	pthread_create(&io_worker1, NULL, &mock_repl_io_worker, args);
	pthread_create(&io_worker2, NULL, &mock_repl_io_worker, args);
	pthread_create(&io_worker3, NULL, &mock_repl_io_worker, args);
	while(1) {
		sleep(5);
		if (rargs->kill_replica == true) {
			REPLICA_ERRLOG("Killing replica:%s port:%d\n",
			    rargs->replica_ip, rargs->replica_port);
			pthread_cond_broadcast(&rargs->mgmt_recv_cv);
			pthread_cond_broadcast(&rargs->mgmt_send_cv);
			pthread_cond_broadcast(&rargs->io_recv_cv);
			pthread_cond_broadcast(&rargs->io_send_cv);
			sleep(5);
			shutdown(rargs->mgmtfd, SHUT_RDWR);
			shutdown(rargs->iofd, SHUT_RDWR);
			close(rargs->mgmtfd);
			close(rargs->iofd);
			close(rargs->file_fd);
			sleep(5);

			while (!TAILQ_EMPTY(&(rargs->mgmt_recv_list))) {
				zio_cmd = TAILQ_FIRST(&rargs->mgmt_recv_list);
				TAILQ_REMOVE(&rargs->mgmt_recv_list, zio_cmd, next);
				free(zio_cmd);
			}

			while (!TAILQ_EMPTY(&rargs->mgmt_send_list)) {
				zio_cmd = TAILQ_FIRST(&rargs->mgmt_send_list);
				TAILQ_REMOVE(&rargs->mgmt_send_list, zio_cmd, next);
				free(zio_cmd);
			}

			while (!TAILQ_EMPTY(&(rargs->io_recv_list))) {
				zio_cmd = TAILQ_FIRST(&rargs->io_recv_list);
				TAILQ_REMOVE(&rargs->io_recv_list, zio_cmd, next);
				free(zio_cmd);
			}

			while (!TAILQ_EMPTY(&rargs->io_send_list)) {
				zio_cmd = TAILQ_FIRST(&rargs->io_send_list);
				TAILQ_REMOVE(&rargs->io_send_list, zio_cmd, next);
				free(zio_cmd);
			}
			rargs->mgmtfd = rargs->iofd = rargs->file_fd = -1;
			pthread_mutex_destroy(&rargs->mgmt_recv_mtx);
			pthread_mutex_destroy(&rargs->mgmt_send_mtx);

			pthread_cond_destroy(&rargs->mgmt_recv_cv);
			pthread_cond_destroy(&rargs->mgmt_send_cv);

			pthread_mutex_destroy(&rargs->io_recv_mtx);
			pthread_mutex_destroy(&rargs->io_send_mtx);

			pthread_cond_destroy(&rargs->io_recv_cv);
			pthread_cond_destroy(&rargs->io_send_cv);
			rargs->kill_is_over = true;
			REPLICA_ERRLOG("Killing of replica:%s port:%d"
			    " completed\n", rargs->replica_ip, rargs->replica_port);
			goto exit;
		}
	}
exit:
	REPLICA_LOG("mock_repl exiting....\n");
	return NULL;
}

/*
 * Create mock replicas given the replica count
 * rargs stores variables, locks, fds needed for replica
 * Each replica will have following threads:
 * - one replica main thread that listens for connections from target
 * - one thread to read mgmt commands requests
 * - one thread to send mgmt commands responses
 * - one thread to work on mgmt commands
 * - one thread to read IOs after connection is established from target
 * - one thread to send IOs response
 * - three threads to work on IOs
 */
static void
create_mock_replicas(int replication_factor, char *volname)
{
	all_rargs = (rargs_t *)malloc(sizeof (rargs_t) * replication_factor);
	all_rthrds = (pthread_t *)malloc(sizeof (pthread_t) * replication_factor);
	rargs_t *rargs;
	char filepath[50];
	int i;

	memset(all_rargs, 0, sizeof (rargs_t) * replication_factor);

	for (i = 0; i < replication_factor; i++) {
		rargs = &(all_rargs[i]);
		strncpy(rargs->replica_ip, "127.0.0.1", MAX_IP_LEN);
		rargs->replica_port = 6161 + i;
		rargs->kill_replica = false;
		rargs->kill_is_over = false;	

		strncpy(rargs->ctrl_ip, "127.0.0.1", MAX_IP_LEN);
		rargs->ctrl_port = 6060;

		strncpy(rargs->volname, volname, MAX_NAME_LEN);

		snprintf(filepath, 45, "/tmp/test_vol%d", (i+1));
		strncpy(rargs->file_path, filepath, MAX_NAME_LEN);

		pthread_create(&all_rthrds[i], NULL, &mock_repl, rargs);
	}
}

static void
usage(void)
{
	printf("istgt_integration -b <blocklen> -s <volsize> -t <total_time_in_sec> -v <volname> -T <testid>\n");
	exit(1);
}

static int
str2shift(const char *buf)
{
	const char *ends = "BKMGTPEZ";
	uint64_t i;

	if (buf[0] == '\0')
		return (0);
	for (i = 0; i < strlen(ends); i++) {
		if (toupper(buf[0]) == ends[i])
			break;
	}
	if (i == strlen(ends)) {
		printf("istgt_it: invalid bytes suffix: %s\n", buf);
		usage();
	}
	if (buf[1] == '\0' || (toupper(buf[1]) == 'B' && buf[2] == '\0')) {
		return (10*i);
	}
	printf("istgt_it: invalid bytes suffix: %s\n", buf);
	usage();
	return (-1);
}

static uint64_t
nicenumtoull(const char *buf)
{
	char *end;
	uint64_t val;

	val = strtoull(buf, &end, 0);
	if (end == buf) {
		printf("istgt_it: bad numeric value: %s\n", buf);
		usage();
	} else if (end[0] == '.') {
		double fval = strtod(buf, &end);
		fval *= pow(2, str2shift(end));
		if (fval > UINT64_MAX) {
			printf("istgt_it: value too large: %s\n", buf);
			usage();
		}
		val = (uint64_t)fval;
	} else {
		int shift = str2shift(end);
		if (shift >= 64 || (val << shift) >> shift != val) {
			printf("istgt_it: value too large: %s\n", buf);
			usage();
		}
		val <<= shift;
	}
	return (val);
}

static void
process_options(int argc, char **argv)
{
	int opt;
	uint64_t val = 0;

	while ((opt = getopt(argc, argv, "b:s:t:T:v:"))
	    != EOF) {
		switch (opt) {
			case 'v':
				break;
			default:
				if (optarg != NULL)
					val = nicenumtoull(optarg);
				break;
		}

		switch (opt) {
			case 'b':
				blocklen = val;
				break;
			case 's':
				volsize = val;
				break;
			case 't':
				total_time_in_sec = val;
				break;
			case 'T':
				test_id = val;
				break;
			case 'v':
				vol_name = optarg;
				break;
			default:
				usage();
		}
	}

	if (volsize == 0)
		volsize = 2*1024ULL*1024ULL*1024ULL;

	printf("vol name: %s volsize: %lu blocklen: %lu\n",
	    vol_name, volsize, blocklen);
	printf("total run time in seconds: %d for test_id: %d\n",
	    total_time_in_sec, test_id);
}

static void *
rebuild_test(void *arg)
{
	rebuild_test_t *test_args = (rebuild_test_t *)arg;
	spec_t *spec = test_args->spec;
	rargs_t *rargs = &(all_rargs[0]);
	pthread_t replica_thread;
	char filepath[50];

	while (1) {
		sleep(5);

		if ((test_args->replica_killing == false) &&
		    (spec->degraded_rcount == 0)) {
			rargs = &(all_rargs[0]);
			rargs->kill_replica = true;
			test_args->replica_killing = true;
			continue;
		}
		
		if (rargs->kill_is_over == true) {
			sleep(60);

			strncpy(rargs->replica_ip, "127.0.0.1", MAX_IP_LEN);
			rargs->replica_port = 6161 + spec->replication_factor + 1;
			rargs->kill_replica = false;
			rargs->kill_is_over = false;	

			strncpy(rargs->ctrl_ip, "127.0.0.1", MAX_IP_LEN);
			rargs->ctrl_port = 6060;

			strncpy(rargs->volname, spec->volname, MAX_NAME_LEN);

			snprintf(filepath, 45, "/tmp/test_vol%d", 1);
			strncpy(rargs->file_path, filepath, MAX_NAME_LEN);

			REPLICA_ERRLOG("Reconnecting new replica:%s port:%d\n",
			    rargs->replica_ip, rargs->replica_port);
			pthread_create(&replica_thread, NULL, &mock_repl, rargs);
			test_args->reregister_replica_test = true;
			continue;
		}

		if ((test_args->replica_killing == true) &&
		    (test_args->reregister_replica_test == true) &&
		    (spec->degraded_rcount == 0)) {
			MTX_LOCK(&test_args->test_mtx);
			pthread_cond_signal(&test_args->test_state_cv);
			MTX_UNLOCK(&test_args->test_mtx);
			goto exit;
		}
	}
exit:
	return NULL;
}


int
main(int argc, char **argv)
{
	int rc;
	spec_t *spec = (spec_t *)malloc(sizeof (spec_t));
	int replication_factor = 3, consistency_factor = 2;
	pthread_t replica_thread;
	struct stat sbuf;
	pthread_t rebuild_test_thread;
	rebuild_test_t *test_args;

	test_args = (rebuild_test_t *)malloc(sizeof (rebuild_test_t));
	rc = pthread_cond_init(&test_args->test_state_cv, NULL);
	if (rc != 0) {
		REPLICA_ERRLOG("cond_init() failed errno:%d\n", errno);
		return -1;
	}

	pthread_mutex_init(&test_args->test_mtx, NULL);
	test_args->spec = spec;
	test_args->replica_killing = false;
	test_args->reregister_replica_test = false;

	process_options(argc, argv);
	rc = pthread_mutexattr_init(&mutex_attr);
	if (rc != 0) {
		REPLICA_ERRLOG("mutexattr_init() failed\n");
		return 1;
	}

#ifdef HAVE_PTHREAD_MUTEX_ADAPTIVE_NP
	rc = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ADAPTIVE_NP);
#else
	rc = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_ERRORCHECK);
#endif

	blocklen = 512;
	/*
	 * We are using /tmp/test_vol* files for replica volume files.
	 */
	if (stat("/tmp/test_vol1", &sbuf)) {
		REPLICA_ERRLOG("volume files (/tmp/test_vol*) not created\n");
		return 1;
	}

	volsize = sbuf.st_size;

	/* Initialize mempool needed for replication*/
	if (initialize_replication_mempool(false)) {
		REPLICA_ERRLOG("Failed to initialize mempool\n");
		return 1;
	}

	initialize_replication();

	rc = initialize_spec(spec);
	if (rc != 0) {
		REPLICA_ERRLOG("error in initializing spec..\n");
		return 1;
	}

	initialize_volume(spec, replication_factor, consistency_factor);

	pthread_create(&replica_thread, NULL, &init_replication, (void *)NULL);

	spec->ready = false;

	create_mock_replicas(spec->replication_factor, spec->volname);
	pthread_create(&rebuild_test_thread, NULL, &rebuild_test, (void *)test_args);

	MTX_LOCK(&test_args->test_mtx);
	pthread_cond_wait(&test_args->test_state_cv, &test_args->test_mtx);
	MTX_UNLOCK(&test_args->test_mtx);

	create_mock_client(spec);

	return 0;
}
