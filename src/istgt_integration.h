#ifndef _ISTGT_INTEGRATION_H
#define	_ISTGT_INTEGRATION_H

#include <stdbool.h>
#include "istgt_lu.h"
#include "istgt_sock.h"
#include "ring_mempool.h"

typedef int (*cstor_listen)(const char *, int, int, int);
typedef int (*cstor_connect)(const char *, int);

TAILQ_HEAD(, istgt_lu_disk_t) spec_q;
pthread_mutex_t specq_mtx;

typedef ISTGT_QUEUE *cmd_ptr;
typedef struct istgt_lu_disk_t spec_t;
typedef struct rcmd_s rcmd_t;

/*
 * state of commands queued for replica according to send/recieve
 */
typedef enum {
	READ_IO_RESP_HDR = 1,	/* to read mgmt IO hdr */
	READ_IO_RESP_DATA = 2,	/* to read handshake msg data */
	WRITE_IO_SEND_HDR = 3,	/* to write mgmt IO hdr */
	WRITE_IO_SEND_DATA = 4,	/* to write mgmt IO data */
} mgmt_cmd_state;

typedef struct mgmt_ack mgmt_ack_data_t;
typedef enum zvol_status replica_state_t;

typedef struct replica_s {
	TAILQ_ENTRY(replica_s) r_next;
	TAILQ_ENTRY(replica_s) r_waitnext;
	rte_smempool_t cmdq;	/* list of IOs queued from spec to replica */
	TAILQ_HEAD(, rcmd_s) readyq;	/* list of IOs (non-blocking IOs) ready to sent to replica */
	TAILQ_HEAD(, rcmd_s) waitq;	/* list of IOs waiting for the response from replica */
	TAILQ_HEAD(, rcmd_s) blockedq;	/* list of blocked IOs */
	pthread_cond_t r_cond;		/* replica level cond. variable */
	pthread_mutex_t r_mtx;		/* replica level mutex lock */
	replica_state_t state;
	spec_t *spec;
	int iofd;
	int mgmt_fd;			/* management connection descriptor */
	int port;			/* replica's IOs server port */
	char *ip;			/* replica's IP */
	uint64_t pool_guid;
	uint64_t zvol_guid;

	void *ongoing_io_buf;		/* payload for current IO response for a replica */
	uint64_t ongoing_io_len;	/* payload size for IO response for a replica */
	rcmd_t *ongoing_io;		/* IO for which we are receiving response from replica */
	void *m_event1;			/* data ptr for epoll */
	int mgmt_eventfd1;		/* eventfd to notify for a command in replica's mgmt cmd queue */
	void *m_event2;			/* data ptr for epoll */
	int mgmt_eventfd2;		/* to inform mgmt interface for an error in data interface */
	int disconnect_conn;		/* to inform data interface for an error in mgmt interface */
	int conn_closed;		/* To track tear down of replica's interfaces(mgmt and data) */
	int epollfd;			/* Epoll descriptor for data interface */
	int data_eventfd;		/* eventfd to notify data interface for IOs in replica's cmdq */
	int epfd;			/* Epoll descriptor for mgmt interface */
	int dont_free;

	struct timespec create_time;

	/*
	 * Following variables should be updated with atomic operation only
	 */
	uint64_t replica_inflight_write_io_cnt;
	uint64_t replica_inflight_read_io_cnt;
	uint64_t replica_inflight_sync_io_cnt;

	zvol_io_hdr_t *io_resp_hdr;	/* header recieved on data connection */
	int io_state;			/* state of command on data connection */
	uint32_t io_read;			/* amount of IO data read in current IO state for data connection */
	zvol_io_hdr_t *mgmt_io_resp_hdr;/* header recieved on management connection */
	void *mgmt_io_resp_data;	/* data recieved on management connection */
	TAILQ_HEAD(, mgmt_cmd_s) mgmt_cmd_queue;	/* mgmt command queue for management connection */
	uint64_t initial_checkpointed_io_seq;
#ifdef	DEBUG
	int replica_mgmt_dport;		/* port from which replica has made mgmt conn. */
#endif
} replica_t;

typedef struct cstor_conn_ops {
	cstor_listen conn_listen;
	cstor_connect conn_connect;
} cstor_conn_ops_t;

void *cleanup_deadlist(void *);
int initialize_replication(void);
int handle_write_resp(spec_t *, replica_t *);
int handle_read_resp(spec_t *, replica_t *);
int update_replica_list(int, spec_t *, int);
replica_t *create_replica_entry(spec_t *, int, int);
int update_replica_entry(spec_t *, replica_t *, int);
int handle_read_data_event(replica_t *);
int handle_write_data_event(replica_t *replica);
void update_volstate(spec_t *);
#endif /* _ISTGT_INTEGRATION_H */
