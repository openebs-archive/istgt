#ifndef _REPLICATION_H
#define	_REPLICATION_H

#include <inttypes.h>
#include <stdint.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/queue.h>
#include <sys/uio.h>
#include <syslog.h>
#include <stdbool.h>
#include "replication_log.h"
#include "zrepl_prot.h"

#define	MAXREPLICA 5
#define	MAXEVENTS 64
#define	BUFSIZE 1024
#define	MAXIPLEN 56
#define	MAXNAMELEN 256

#include "istgt_lu.h"

/*
 * NOTE : RCMD_MEMPOOL_ENTRIES depends on number of replicas ISGT can support
 * current limit is 524288 per replica. Replica will be able to serve
 * 524288 in-flight IOs.
 */
#define	RCMD_MEMPOOL_ENTRIES    (1 << 19)

#define	MAX_OF(a, b) (((a) > (b))?(a):(b))

typedef enum zvol_cmd_type_e {
	CMD_IO = 1,
	CND_MGMT,
} zvol_cmd_type_t;

typedef enum rcomm_cmd_state_s {
	CMD_CREATED = 1,
	CMD_ENQUEUED_TO_WAITQ,
	CMD_ENQUEUED_TO_PENDINGQ,
	CMD_EXECUTION_DONE,
} rcomm_cmd_state_t;

typedef enum rcmd_state_s {
	RECEIVED_OK = 1 << 0,
	RECEIVED_ERR = 1 << 1,
	SENT_TO_HEALTHY = 1 << 2,
	SENT_TO_DEGRADED = 1 << 3,
} rcmd_state_t;

typedef struct resp_data {
	void *data;
	uint64_t len;
} resp_data_t;

struct replica_rcomm_resp {
	zvol_io_hdr_t io_resp_hdr;
	uint8_t *data_ptr;
	rcmd_state_t status;
} __attribute__((packed));

typedef struct replica_rcomm_resp replica_rcomm_resp_t;

typedef struct rcommon_cmd_s {
	TAILQ_ENTRY(rcommon_cmd_s)  wait_cmd_next; /* for rcommon_waitq */
	int luworker_id;
	int copies_sent;
	uint8_t replication_factor;
	uint8_t consistency_factor;
	zvol_op_code_t opcode;
	int healthy_count;	/* number of healthy replica when cmd queued */
	uint64_t io_seq;
	uint64_t lun_id;
	uint64_t offset;
	uint64_t data_len;
	uint64_t total_len;
	rcomm_cmd_state_t state;
	void *data;
	pthread_mutex_t *mutex;
	pthread_cond_t *cond_var;
	/* array of response received from replica */
	replica_rcomm_resp_t resp_list[MAXREPLICA];
	int64_t iovcnt;
	struct iovec iov[41];
} rcommon_cmd_t;

typedef struct rcmd_s {
	TAILQ_ENTRY(rcmd_s)  next;
	zvol_op_code_t opcode;
	uint64_t io_seq;
	void *rcommq_ptr;
	uint8_t *iov_data;	/* for header to be sent to replica */
	int healthy_count;	/* number of healthy replica when cmd queued */
	int idx;		/* index for rcommon_cmd in resp_list */
	int64_t iovcnt;
	uint64_t offset;
	uint64_t data_len;
	struct iovec iov[41];
	struct timespec queued_time;
} rcmd_t;

typedef struct replica_s replica_t;

typedef struct istgt_lu_disk_t spec_t;

typedef struct io_data_chunk {
	TAILQ_ENTRY(io_data_chunk) io_data_chunk_next;
	uint64_t io_num;
	uint8_t *data;
} io_data_chunk_t;

TAILQ_HEAD(io_data_chunk_list_t, io_data_chunk);
/*
 * struct can be used in multithreaded scope and in single thread scope.
 * Multithreaded scope - snapshot create
 * Single threade scope - prepare_for_rebuild
 * So if you are using it in multithreaded scope, use mutex for
 * thread safe access.
 */
typedef struct rcommon_mgmt_cmd {
	int cmds_sent; // total cmds sent
	int cmds_succeeded; // success responses received
	int cmds_failed; // failure responses received
	int caller_gone; // thread that is waiting for responses is gone?
	uint64_t buf_size;
	pthread_mutex_t mtx;
	void *buf;
} rcommon_mgmt_cmd_t;

typedef struct mgmt_cmd_s {
	TAILQ_ENTRY(mgmt_cmd_s) mgmt_cmd_next;
	zvol_io_hdr_t *io_hdr;			/* management command header */
	void *data;				/* cmd data */
	int mgmt_cmd_state;			/* current state of cmd */
	int cmd_completed;	/* if command execution has completed */
	/*
	 * amount of IO data written/read in current command state
	 */
	int io_bytes;
	rcommon_mgmt_cmd_t *rcomm_mgmt;
} mgmt_cmd_t;

typedef struct io_event {
	int fd;
	int *state;
	zvol_io_hdr_t *io_hdr;
	void **io_data;
	int *byte_count;
} io_event_t;

typedef struct mgmt_event {
	int fd;
	replica_t *r_ptr;
} mgmt_event_t;

typedef struct known_replica_s {
	TAILQ_ENTRY(known_replica_s) next;
	int is_connected;
	uint64_t zvol_guid;
} known_replica_t;

extern struct timespec istgt_start_time;

void *init_replication(void *);
int make_socket_non_blocking(int);
int send_mgmtack(int, zvol_op_code_t, void *, char *, int);
int zvol_handshake(spec_t *, replica_t *);
void accept_mgmt_conns(int, int);
void clear_rcomm_cmd(rcommon_cmd_t *);
void ask_replica_status(spec_t *spec, replica_t *replica);
extern void * replica_thread(void *);
extern int do_drainfd(int);
void close_fd(int epollfd, int fd);
int64_t perform_read_write_on_fd(int fd, uint8_t *data, uint64_t len,
    int state);
int initialize_volume(spec_t *spec, int, int);
void destroy_volume(spec_t *spec);
void inform_mgmt_conn(replica_t *r);
extern const char * get_cv_status(spec_t *spec, int replica_cnt, int healthy_replica_cnt);

/* Replica default timeout is 200 seconds */
#define	REPLICA_DEFAULT_TIMEOUT	200

// Volume status
#define VOL_STATUS_OFFLINE "Offline"
#define VOL_STATUS_DEGRADED "Degraded"
#define VOL_STATUS_HEALTHY "Healthy"
// Replica status
#define REPLICA_STATUS_DEGRADED "Degraded"
#define REPLICA_STATUS_HEALTHY "Healthy"

#define	DECREMENT_INFLIGHT_REPLICA_IO_CNT(_r, _opcode)			\
	do {								\
		switch (_opcode) {					\
			case ZVOL_OPCODE_WRITE:				\
				__sync_fetch_and_sub(			\
				    &_r->replica_inflight_write_io_cnt,	\
				    1);					\
				break;					\
									\
			case ZVOL_OPCODE_READ:				\
				__sync_fetch_and_sub(			\
				    &_r->replica_inflight_read_io_cnt,	\
				    1);					\
				break;					\
									\
			case ZVOL_OPCODE_SYNC:				\
				__sync_fetch_and_sub(			\
				    &_r->replica_inflight_sync_io_cnt,	\
				    1);					\
				break;					\
									\
			default:					\
				break;					\
		}							\
	} while (0)

#define	INCREMENT_INFLIGHT_REPLICA_IO_CNT(_r, _opcode)			\
	do {								\
		switch (_opcode) {					\
			case ZVOL_OPCODE_WRITE:				\
				__sync_fetch_and_add(			\
				    &_r->replica_inflight_write_io_cnt,	\
				    1);					\
				break;					\
									\
			case ZVOL_OPCODE_READ:				\
				__sync_fetch_and_add(			\
				    &_r->replica_inflight_read_io_cnt,	\
				    1);					\
				break;					\
									\
			case ZVOL_OPCODE_SYNC:				\
				__sync_fetch_and_add(			\
				    &_r->replica_inflight_sync_io_cnt,	\
				    1);					\
				break;					\
									\
			default:					\
				break;					\
		}							\
	} while (0)

#endif /* _REPLICATION_H */
