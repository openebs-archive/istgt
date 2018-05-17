
#ifndef ISCSI
#define ISCSI 1
#include "istgt_lu.h"
#include "istgt_sock.h"
#include <stdbool.h>

extern pthread_t timerthread, replication_thread;
typedef int (*cstor_listen)(const char *, int, int);
typedef int (*cstor_connect)(const char *, int);
typedef void (*cstor_read)(void *, void *, void *, void *, uint64_t);
typedef void (*cstor_close)(void *, void *, void *, void *, uint64_t);

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
	TAILQ_HEAD(, rcmd_s) sendq;
	TAILQ_HEAD(, rcmd_s) waitq;
	TAILQ_HEAD(, rcmd_s) blockedq;
	TAILQ_HEAD(, rcmd_s) read_waitq;
	pthread_cond_t r_cond;
	pthread_mutex_t r_mtx;
	replica_state_t state;
	spec_t *spec;
	int id;
	int iofd;
	int epfd;
	int mgmt_fd;
	int sender_epfd;
	int port;
	char *ip;
	uint64_t least_recvd;
	uint64_t pool_guid;
	uint64_t zvol_guid;
	int cur_recvd;
	uint64_t rrio_seq;
	uint64_t wrio_seq;

	zvol_io_hdr_t *io_resp_hdr;	// header recieved on data connection
	void *io_resp_data;		// data recieved on data connection
	int io_state;			// state of command on data connection
	int io_read;			// amount of IO data read in current IO state for data connection
	bool removed;			// If replica is removed from spec queue or not
	zvol_io_hdr_t *mgmt_io_resp_hdr;// header recieved on management connection
	void *mgmt_io_resp_data;	// data recieved on management connection
	TAILQ_HEAD(, mgmt_cmd_s) mgmt_cmd_queue;	// command queue for management connection
	uint64_t initial_checkpointed_io_seq;
} replica_t;

typedef struct cstor_conn_ops {
	cstor_listen conn_listen;
	cstor_connect conn_connect;
	//	cstor_read conn_read;
	//	cstor_close conn_close;
} cstor_conn_ops_t;

int initialize_volume(spec_t *);
void *replicator(void *);
void *replica_sender(void *);
void *replica_receiver(void *);
int initialize_replication(void);
int handle_write_resp(spec_t *, replica_t *);
int handle_read_resp(spec_t *, replica_t *);
int update_replica_list(int, spec_t *, int);
int remove_replica_from_list(spec_t *, int);
void unblock_blocked_cmds(replica_t *);

replica_t *create_replica_entry(spec_t *, int, int);
replica_t *update_replica_entry(spec_t *, replica_t *, int);

replica_t * get_replica(int mgmt_fd, spec_t **);
void handle_read_data_event(replica_t *);
int handle_write_data_event(replica_t *replica);

void update_volstate(spec_t *);
void clear_replica_cmd(spec_t *, rcmd_t *);
#endif
