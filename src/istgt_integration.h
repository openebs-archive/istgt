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
	TAILQ_HEAD(, rcmd_s) waitq;
	TAILQ_HEAD(, rcmd_s) blockedq;
	pthread_cond_t r_cond;
	pthread_mutex_t r_mtx;
	replica_state_t state;
	spec_t *spec;
	int iofd;
	int mgmt_fd;
	int port;
	char *ip;
	uint64_t pool_guid;
	uint64_t zvol_guid;

	void *ongoing_io_buf;	// data received from replica
	uint64_t ongoing_io_len;	// number of bytes received from replica
	rcmd_t *ongoing_io;		// cmd for which we are receiving response
	void *m_event1;
	int mgmt_eventfd1;
	void *m_event2;
	int mgmt_eventfd2;
	int disconnect_conn;
	int conn_closed;
	int epollfd;
	int data_eventfd;
	int epfd;
	int dont_free;

	struct timespec create_time;
	/*
	 * this variable will be updated by only replica_thread,
	 * so no lock required while updating this
	 */
	uint64_t replica_inflight_write_io_cnt;

	zvol_io_hdr_t *io_resp_hdr;	// header recieved on data connection
	int io_state;			// state of command on data connection
	int io_read;			// amount of IO data read in current IO state for data connection
	zvol_io_hdr_t *mgmt_io_resp_hdr;// header recieved on management connection
	void *mgmt_io_resp_data;	// data recieved on management connection
	TAILQ_HEAD(, mgmt_cmd_s) mgmt_cmd_queue;	// command queue for management connection
	uint64_t initial_checkpointed_io_seq;
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
