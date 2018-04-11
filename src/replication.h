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
#include "istgt_integration.h"
#include "istgt_lu.h"

#ifndef REPLICA_INITIALIZE
#define REPLICA_INITIALIZE 1
#define MAXREPLICA 64
#define MAXEVENTS 64
#define BUFSIZE 1024
#define MAXIPLEN 56
#define MAXNAMELEN 256

#define MAX(a,b) (((a)>(b))?(a):(b))

typedef enum zvol_op_code_e {
	ZVOL_OPCODE_HANDSHAKE = 1,
	ZVOL_OPCODE_READ,
	ZVOL_OPCODE_WRITE,
	ZVOL_OPCODE_UNMAP,
	ZVOL_OPCODE_SYNC,
	ZVOL_OPCODE_SNAP_CREATE,
	ZVOL_OPCODE_SNAP_ROLLBACK,
} zvol_op_code_t;

typedef enum zvol_cmd_type_e {
	CMD_IO = 1,
	CND_MGMT,
} zvol_cmd_type_t;

typedef struct mgmt_ack_data_s {
	char volname[MAXNAMELEN];
	char ip[MAXIPLEN];
	int port;
} mgmt_ack_data_t;

typedef enum cmd_state_s {
	CMD_CREATED = 1,
	CMD_ENQUEUED_TO_WAITQ,
	CMD_ENQUEUED_TO_PENDINGQ,
	CMD_EXECUTION_DONE,
} cmd_state_t;

typedef struct rcommon_cmd_s {
	TAILQ_ENTRY(rcommon_cmd_s)  send_cmd_next; /* for rcommon_sendq */
	TAILQ_ENTRY(rcommon_cmd_s)  wait_cmd_next; /* for rcommon_waitq */
	TAILQ_ENTRY(rcommon_cmd_s)  pending_cmd_next; /* for rcommon_pendingq */
	int luworker_id;
	int acks_recvd;
	int ios_aborted;
	int copies_sent;
	zvol_op_code_t opcode;
	uint64_t io_seq;
	uint64_t lun_id;
	uint64_t offset;
	uint64_t data_len;
	uint64_t total_len;
	int status;
	bool completed;
	cmd_state_t state;
	void *data;
	uint64_t total;
	int64_t iovcnt;
	uint64_t bitset;
	struct iovec iov[21];
} rcommon_cmd_t;

typedef struct rcmd_s {
	TAILQ_ENTRY(rcmd_s)  rsend_cmd_next; /* for replica sendq */
	TAILQ_ENTRY(rcmd_s)  rwait_cmd_next; /* for replica waitq */
	TAILQ_ENTRY(rcmd_s)  rblocked_cmd_next; /* for replica blockedq */
	TAILQ_ENTRY(rcmd_s)  rread_cmd_next; /* for replica read_waitq */
	zvol_op_code_t opcode;
	uint64_t io_seq;
	uint64_t rrio_seq;
	uint64_t wrio_seq;
	void *rcommq_ptr;
	bool ack_recvd;
	int status;
	int64_t iovcnt;
	uint64_t offset;
	uint64_t data_len;
	struct iovec iov[21];
} rcmd_t;

typedef enum zvol_op_status_e {
	ZVOL_OP_STATUS_OK = 1,
	ZVOL_OP_STATUS_FAILED,
} zvol_op_status_t;

typedef struct zvol_io_hdr_s {
	zvol_op_code_t opcode;
	uint64_t io_seq;
	uint64_t offset;
	uint64_t len;
	rcmd_t *q_ptr;
	zvol_op_status_t status;
} zvol_io_hdr_t;

typedef struct replica_s replica_t;
typedef struct istgt_lu_disk_t spec_t;

void *init_replication(void *);
int sendio(int, int, rcommon_cmd_t *, rcmd_t *);
int send_mgmtio(int, zvol_op_code_t, void *, uint64_t);
int make_socket_non_blocking(int);
int send_mgmtack(int, zvol_op_code_t, void *, char *, int);
int wait_for_fd(int);
int64_t read_data(int, uint8_t *, uint64_t, int *, int *);
int zvol_handshake(spec_t *, replica_t *);
void accept_mgmt_conns(int, int);
int send_io_resp(int fd, zvol_io_hdr_t *, void *);
int initialize_replication_mempool(bool should_fail);
int destroy_relication_mempool(void);
int allocate_replica_id(spec_t *);
void release_replica_id(spec_t *, int);
void cleanup_replica(replica_t *);
replica_t *get_next_replica(spec_t *, replica_t *);
#define REPLICA_LOG(fmt, ...)  syslog(LOG_NOTICE, 	 "%-18.18s:%4d: %-20.20s: " fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define REPLICA_NOTICELOG(fmt, ...) syslog(LOG_NOTICE, "%-18.18s:%4d: %-20.20s: " fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define REPLICA_ERRLOG(fmt, ...) syslog(LOG_ERR,  	 "%-18.18s:%4d: %-20.20s: " fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)
#define REPLICA_WARNLOG(fmt, ...) syslog(LOG_ERR, 	 "%-18.18s:%4d: %-20.20s: " fmt, __func__, __LINE__, tinfo, ##__VA_ARGS__)

#endif
