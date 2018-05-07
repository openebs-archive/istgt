#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <stdlib.h>
#include <unistd.h>

#include "replication.h"
#include "istgt_integration.h"
#include "replication_misc.h"
int64_t id;
uint64_t vol_size;
uint64_t blocklen;
uint64_t *hash_buf;

cstor_conn_ops_t cstor_ops = {
	.conn_listen = replication_listen,
	.conn_connect = replication_connect,
};
__thread char  tinfo[20] =  {0};
#define build_mgmt_ack_hdr {\
	mgmt_ack_hdr = (zvol_io_hdr_t *)malloc(sizeof(zvol_io_hdr_t));\
	mgmt_ack_hdr->opcode = opcode;\
	mgmt_ack_hdr->version = REPLICA_VERSION;\
	mgmt_ack_hdr->len = sizeof(mgmt_ack_data_t);\
	mgmt_ack_hdr->status = ZVOL_OP_STATUS_OK;\
	mgmt_ack_hdr->checkpointed_io_seq = 1000;\
}

#define build_mgmt_ack_data {\
	mgmt_ack_data = (mgmt_ack_t *)malloc(sizeof(mgmt_ack_t));\
	strcpy(mgmt_ack_data->ip, replicaip);\
	strcpy(mgmt_ack_data->volname, buf);\
	mgmt_ack_data->port = replica_port;\
	mgmt_ack_data->pool_guid = 100;\
	mgmt_ack_data->zvol_guid = 500;\
}

int64_t test_read_data(int fd, uint8_t *data, uint64_t len);

int64_t
test_read_data(int fd, uint8_t *data, uint64_t len) {
	int rc;
	uint64_t nbytes = 0;
	while((rc = read(fd, data + nbytes, len - nbytes))) {
		if(rc < 0) {
			if(nbytes > 0 && errno == EAGAIN) {
				return nbytes;
			} else {
				return -1;
			}
		}
		nbytes += rc;
		if(nbytes == len) {
			break;
		}
	}
	return nbytes;
}

int
send_mgmtack(int fd, zvol_op_code_t opcode, void *buf, char *replicaip, int replica_port)
{
	zvol_io_hdr_t *mgmt_ack_hdr = NULL;
	int i, nbytes = 0;
	int rc = 0, start;
	struct iovec iovec[6];
	build_mgmt_ack_hdr;
	int iovec_count;
	zrepl_status_ack_t repl_status;
	mgmt_ack_t *mgmt_ack_data = NULL;
	int ret = -1;

	iovec[0].iov_base = mgmt_ack_hdr;
	iovec[0].iov_len = 16;

	iovec[1].iov_base = ((uint8_t *)mgmt_ack_hdr) + 16;
	iovec[1].iov_len = 16;

	iovec[2].iov_base = ((uint8_t *)mgmt_ack_hdr) + 32;
	iovec[2].iov_len = sizeof (zvol_io_hdr_t) - 32;

	if (opcode == ZVOL_OPCODE_REPLICA_STATUS) {
		repl_status.state = ZVOL_STATUS_HEALTHY;
		mgmt_ack_hdr->len = sizeof(zrepl_status_ack_t);
		iovec_count = 4;
		iovec[3].iov_base = &repl_status;
		iovec[3].iov_len = sizeof(zrepl_status_ack_t);
	} else {
		build_mgmt_ack_data;

		iovec[3].iov_base = mgmt_ack_data;
		iovec[3].iov_len = 50;

		iovec[4].iov_base = ((uint8_t *)mgmt_ack_data) + 50;
		iovec[4].iov_len = 50;

		iovec[5].iov_base = ((uint8_t *)mgmt_ack_data) + 100;
		iovec[5].iov_len = sizeof (mgmt_ack_t) - 100;
		iovec_count = 6;
	}

	for (start = 0; start < iovec_count; start += 2) {
		nbytes = iovec[start].iov_len + iovec[start + 1].iov_len;
		while (nbytes) {
			rc = writev(fd, &iovec[start], 2);//Review iovec in this line
			if (rc < 0) {
				goto out;
			}
			nbytes -= rc;
			if (nbytes == 0)
				break;
			/* adjust iovec length */
			for (i = start; i < start + 2; i++) {
				if (iovec[i].iov_len != 0 && iovec[i].iov_len > (size_t)rc) {
					iovec[i].iov_base
						= (void *) (((uintptr_t)iovec[i].iov_base) + rc);
					iovec[i].iov_len -= rc;
					break;
				} else {
					rc -= iovec[i].iov_len;
					iovec[i].iov_len = 0;
				}
			}
		}
	}
	ret = 0;
out:
	if (mgmt_ack_hdr)
		free(mgmt_ack_hdr);

	if (mgmt_ack_data)
		free(mgmt_ack_data);

	return ret;
}
#include <stdlib.h>
int
send_io_resp(int fd, zvol_io_hdr_t *io_hdr, void *buf)
{
	struct iovec *iovec = NULL;
	struct zvol_io_rw_hdr *io_rw_hdr;
	int64_t iovcnt, i, nbytes = 0;
	int rc = 0;
	uint64_t bytes_written;
	int64_t io_num = -1;
	uint64_t data_len = 0, iovec_size, iovec_indx, offset = 0;
	io_hdr->status = ZVOL_OP_STATUS_OK;
	if(io_hdr->opcode == ZVOL_OPCODE_READ) {
		bytes_written = 0;
		data_len = 0;
		offset = io_hdr->offset;
		iovec_size = 3;
		iovec_indx = 0;
		nbytes = io_hdr->len;
		iovec = (struct iovec *)malloc(iovec_size *sizeof(struct iovec));
		while(nbytes) {
			if((int64_t)hash_buf[offset%blocklen] != io_num) {
				io_num = hash_buf[offset%blocklen];
				data_len += sizeof(struct zvol_io_rw_hdr) + blocklen;
				if(iovec_indx + 3 >= iovec_size) {
					iovec_size *= 2;
					iovec = (struct iovec *)realloc((void *)iovec, iovec_size*sizeof(struct iovec));
				}
				iovec[++iovec_indx].iov_base = io_rw_hdr = (struct zvol_io_rw_hdr *)malloc(sizeof(struct zvol_io_rw_hdr));
				iovec[iovec_indx].iov_len = sizeof(struct zvol_io_rw_hdr);
				iovec[++iovec_indx].iov_base = (uint8_t *)buf + bytes_written;
				iovec[iovec_indx].iov_len = blocklen;
				io_rw_hdr->io_num = io_num;
				io_rw_hdr->len = blocklen;
			} else {
				data_len += blocklen;
				iovec[iovec_indx].iov_len += blocklen;
				io_rw_hdr->len += blocklen;
			}
			nbytes -= blocklen;
			offset += blocklen;
			bytes_written += blocklen;
		}
		io_hdr->len = data_len;
		iovec[0].iov_base = io_hdr;
		iovec[0].iov_len = sizeof(zvol_io_hdr_t);
		iovcnt = iovec_indx + 1;
		nbytes = data_len + sizeof(zvol_io_hdr_t);
	} else if(io_hdr->opcode == ZVOL_OPCODE_WRITE) {
		iovcnt = 1;
		iovec = (struct iovec *)malloc(iovcnt *sizeof(struct iovec));
		iovec[0].iov_base = io_hdr;
		nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
	} else {
		iovcnt = 1;
		iovec = (struct iovec *)malloc(iovcnt *sizeof(struct iovec));
		iovec[0].iov_base = io_hdr;
		nbytes = iovec[0].iov_len = sizeof(zvol_io_hdr_t);
		io_hdr->len = 0;
	}
	while (nbytes) {
		rc = writev(fd, iovec, iovcnt);//Review iovec in this line
		if (rc < 0) {
			REPLICA_LOG("ERROR: %d\n", errno);
			free(iovec);
			return -1;
		}
		nbytes -= rc;
		if (nbytes == 0)
			break;
		/* adjust iovec length */
		for (i = 0; i < iovcnt; i++) {
			if (iovec[i].iov_len != 0 && iovec[i].iov_len > (size_t)rc) {
				iovec[i].iov_base
					= (void *) (((uintptr_t)iovec[i].iov_base) + rc);
				iovec[i].iov_len -= rc;
				break;
			} else {
				rc -= iovec[i].iov_len;
				iovec[i].iov_len = 0;
			}
		}
	}
	free(iovec);
	return 0;

}

int
main(int argc, char **argv)
{
	if(argc < 9) {
		exit(EXIT_FAILURE);
	}
	uint64_t offset;
	char *ctrl_ip = argv[1];
	int ctrl_port = atoi(argv[2]);
	char *replicaip = argv[3];
	int replica_port = atoi(argv[4]);
	char *test_vol = argv[5];
	char *test_hash = argv[6];
	int sleeptime = 0;
	struct zvol_io_rw_hdr *io_rw_hdr;
	vol_size = strtoul(argv[7], NULL, 10);
	blocklen = strtoul(argv[8], NULL, 10);
	//id = atoi(argv[9]);
	if (argc == 11)
		sleeptime = atoi(argv[10]);

	hash_buf = (uint64_t *)malloc(64 * (vol_size/blocklen));
	memset(hash_buf, 0, 64 * (vol_size/blocklen));
	int iofd, mgmtfd, sfd, rc, epfd, event_count, i;
	int64_t count;
	struct epoll_event event, *events;
	uint8_t *data, *data_ptr_cpy;
	uint64_t data_len, nbytes = 0;
	char *volname;
	int vol_fd = open(test_vol, O_RDWR, 0666);

	int hash_fd = open(test_hash, O_RDWR, 0666);
	uint64_t len = 64 * (vol_size/blocklen);
	offset = 0;
	nbytes = 0;
	while((rc = pread(hash_fd, hash_buf + nbytes, len - nbytes, offset + nbytes))) {
		if(rc == -1 ) {
			if(errno == EAGAIN) {
				sleep(1);
				continue;
			}
			break;
		}
		nbytes += rc;
		if(nbytes == len) {
			break;
		}
	}
	zvol_op_code_t opcode;
	zvol_io_hdr_t *io_hdr = malloc(sizeof(zvol_io_hdr_t));
	zvol_io_hdr_t *mgmtio = malloc(sizeof(zvol_io_hdr_t));
	
	data = NULL;
	bool read_rem_data = false;
	bool read_rem_hdr = false;
	uint64_t recv_len = 0;
	uint64_t total_len = 0;
	uint64_t io_hdr_len = sizeof(zvol_io_hdr_t);
	

	epfd = epoll_create1(0);
	
	//Create listener for io connections from controller and add to epoll
	if((sfd = cstor_ops.conn_listen(replicaip, replica_port, 32)) < 0) {
                REPLICA_LOG("conn_listen() failed, errorno:%d", errno);
                exit(EXIT_FAILURE);
        }
	event.data.fd = sfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, sfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, errrno:%d", errno);
		exit(EXIT_FAILURE);
	}

	//Connect to controller to start handshake and connect to epoll
	if((mgmtfd = cstor_ops.conn_connect(ctrl_ip, ctrl_port)) < 0) {
		REPLICA_ERRLOG("conn_connect() failed errno:%d\n", errno);
		exit(EXIT_FAILURE);
	}
	event.data.fd = mgmtfd;
	event.events = EPOLLIN | EPOLLET;
	rc = epoll_ctl(epfd, EPOLL_CTL_ADD, mgmtfd, &event);
	if (rc == -1) {
		REPLICA_ERRLOG("epoll_ctl() failed, errrno:%d", errno);
		exit(EXIT_FAILURE);
	}

	events = calloc(MAXEVENTS, sizeof(event));
	while (1) {
		event_count = epoll_wait(epfd, events, MAXEVENTS, -1);
		for(i=0; i< event_count; i++) {
			if ((events[i].events & EPOLLERR) ||
					(events[i].events & EPOLLHUP) ||
					(!(events[i].events & EPOLLIN))) {
				fprintf (stderr, "epoll error\n");
				continue;
			} else if (events[i].data.fd == mgmtfd) {
				count = test_read_data(events[i].data.fd, (uint8_t *)mgmtio, sizeof(zvol_io_hdr_t));
				if (count<0)
				{
					if (errno != EAGAIN)
					{
						perror("read");
						//done = 1;
					}
					break;
				}
				if(mgmtio->len) {
					data = data_ptr_cpy = malloc(mgmtio->len);
					data_len = mgmtio->len;
					count = test_read_data(events[i].data.fd, (uint8_t *)data, mgmtio->len);
				}
				opcode = mgmtio->opcode;
				volname = (char *)data;
				send_mgmtack(mgmtfd, opcode, data, replicaip, replica_port);
			} else if (events[i].data.fd == sfd) {
				struct sockaddr saddr;
				socklen_t slen;
				char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
				slen = sizeof(saddr);
				iofd = accept(sfd, &saddr, &slen);
				if (iofd == -1) {
					if((errno == EAGAIN) || (errno == EWOULDBLOCK)) {
						break;
					} else {
						REPLICA_ERRLOG("accept() failed, errrno:%d", errno);
						break;
					}
				}

				rc = getnameinfo(&saddr, slen,
						hbuf, sizeof(hbuf),
						sbuf, sizeof(sbuf),
						NI_NUMERICHOST | NI_NUMERICSERV);
				if (rc == 0) {
					REPLICA_LOG("Accepted connection on descriptor %d "
							"(host=%s, port=%s)\n", iofd, hbuf, sbuf);
				}
				rc = make_socket_non_blocking(iofd);
				if (rc == -1) {
					REPLICA_ERRLOG("make_socket_non_blocking() failed, errno:%d", errno);
					exit(EXIT_FAILURE);
				}
				event.data.fd = iofd;
				event.events = EPOLLIN | EPOLLET;
				rc = epoll_ctl(epfd, EPOLL_CTL_ADD, iofd, &event);
				if(rc == -1) {
					REPLICA_ERRLOG("epoll_ctl() failed, errno:%d", errno);
					exit(EXIT_FAILURE);
				}
			} else if(events[i].data.fd == iofd) {
				while(1) {
					if(read_rem_data) {
						count = test_read_data(events[i].data.fd, (uint8_t *)data + recv_len, total_len - recv_len);
						if(count < 0 && errno == EAGAIN) {
							break;
						}else if(((uint64_t)count < total_len - recv_len) && errno == EAGAIN) {
							recv_len += count;
							break;
						} else {
							recv_len = 0;
							total_len = 0;
							read_rem_data = false;
							goto execute_io;
						}

					} else if(read_rem_hdr) {
						count = test_read_data(events[i].data.fd, (uint8_t *)io_hdr + recv_len, total_len - recv_len);
						if(count < 0 && errno == EAGAIN) {
							break;
						} else if(((uint64_t)count < total_len - recv_len) && errno == EAGAIN) {
							recv_len += count;
							break;
						} else {
							read_rem_hdr = false;
							recv_len = 0;
							total_len = 0;
						}
					} else {
						count = test_read_data(events[i].data.fd, (uint8_t *)io_hdr, io_hdr_len);
						if((count < 0) && (errno == EAGAIN)) {
							break;
						} else if(((uint64_t)count < io_hdr_len) && (errno == EAGAIN)) {
							read_rem_hdr = true;
							recv_len = count;
							total_len = io_hdr_len;
							break;
						}
						read_rem_hdr = false;
					}

					if(io_hdr->opcode == ZVOL_OPCODE_WRITE || io_hdr->opcode == ZVOL_OPCODE_HANDSHAKE) {
						if(io_hdr->len) {
							data = malloc(io_hdr->len);
							nbytes = 0;
							count = test_read_data(events[i].data.fd, (uint8_t *)data, io_hdr->len);
							if (count == -1 && errno == EAGAIN) {
								read_rem_data = true;
								recv_len = 0;
								total_len = io_hdr->len;
								break;
							} else if((uint64_t)count < io_hdr->len && errno == EAGAIN) {
								read_rem_data = true;
								recv_len = count;
								total_len = io_hdr->len;
								break;
							}
							read_rem_data = false;
						}
					}
execute_io:
					if(io_hdr->opcode == ZVOL_OPCODE_WRITE) {
						io_rw_hdr = (struct zvol_io_rw_hdr *)data;
						data += sizeof(struct zvol_io_rw_hdr);
						offset = io_hdr->offset;
						nbytes = io_rw_hdr->len;
						while(nbytes > 0) {
							pwrite(hash_fd, &io_rw_hdr->io_num,  64, (offset%blocklen)*64);
							hash_buf[offset%blocklen] = io_rw_hdr->io_num;
							offset += blocklen;
							nbytes -= blocklen;
						}
						nbytes = 0;
						while((rc = pwrite(vol_fd, data + nbytes, io_rw_hdr->len - nbytes, io_hdr->offset + nbytes))) {
							if(rc == -1 ) {
								if(errno == 11) {
									sleep(1);
									continue;
								}
								break;
							}
							nbytes += rc;
							if(nbytes == io_rw_hdr->len) {
								break;
							}
						}
						data -= sizeof(struct zvol_io_rw_hdr);
						usleep(sleeptime);
					} else if(io_hdr->opcode == ZVOL_OPCODE_READ) {
						if(io_hdr->len) {
							data = malloc(io_hdr->len);
						}
						nbytes = 0;
						while((rc = pread(vol_fd, data + nbytes, io_hdr->len - nbytes, io_hdr->offset + nbytes))) {
							if(rc == -1 ) {
								if(errno == EAGAIN) {
									sleep(1);
									continue;
								}
								break;
							}
							nbytes += rc;
							if(nbytes == io_hdr->len) {
								break;
							}
						}
					}
					send_io_resp(iofd, io_hdr, data);
					if (data) {
						free(data);
						data = NULL;
					}
				}
			}
		}
	}
	return 0;
}
