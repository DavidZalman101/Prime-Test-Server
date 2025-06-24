/*
 * Skeleton for prime server
 * Complete this implementation using libaco and io_uring
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <stdbool.h>
#include <ctype.h>
#include <assert.h>

#include "liburing.h"
#include "aco.h"

#define SOCKET_PATH "/tmp/prime_server.sock"
#define MAX_CLIENTS 128
#define BUFFER_SIZE 1024
#define QUEUE_DEPTH 516
#define COROUTINE_COUNTER_LIMIT 100000000
#define REQUESTS_HANDLER_BATCH_SIZE 3

// Operation types
enum {
	EVENT_TYPE_ACCEPT,
	EVENT_TYPE_READ,
	EVENT_TYPE_WRITE,
	EVENT_TYPE_CLOSE,
	EVENT_TYPE_PROV_BUF,
};

// Client Stage
enum {
	STAGE_IDLE,
	STAGE_READ,
	STAGE_CALC,
	STAGE_WRITE,
	STAGE_CLOSE,
};

struct request {
	int event_type;
	int client_idx;
};

typedef struct conn_info {
	int client_idx;
	int type;
} conn_info;

typedef struct {
	/* Client stage */
	int stage;

	/* comm data */
	int fd;
	int buf_idx;
	int buffer_len;
	char response[3];

	/* coro data */
	int idx;
	aco_t *coro;
	aco_share_stack_t *sstk;
} client_t;

// Global state
struct io_uring ring;
client_t clients[MAX_CLIENTS];
char bufs[MAX_CLIENTS][BUFFER_SIZE];
int server_fd;
int group_id = 1337;
aco_t *main_co;

typedef enum {
    MODE_THROUGHPUT,
    MODE_LOW_LATENCY
} opt_mode_t;

opt_mode_t g_mode = MODE_THROUGHPUT;

// Function declarations
void is_prime(void);
int setup_server_socket(void);
client_t *find_free_client(void);

void add_accept_request(void);
void add_read_request(client_t *);
void add_write_request(client_t *);
void add_provide_buf(client_t *);
void add_close_request(client_t *);

void handle_request(struct io_uring_cqe *);
void handle_requests(void);
void handle_prime_checks(void);

void server_cleanup_and_exit(int);
void clear_client(client_t *);
void create_coroutine(client_t *);

// Prime checking - DO NOT CHANGE THE ALGORITHM
// You can add yield points to improve performance
void
is_prime(void) {
	bool is_prime_res = false;
	int idx = *(int*)aco_get_arg();
	assert(idx >= 0 && idx < MAX_CLIENTS);
	client_t *client = &clients[idx];
	assert(client->stage == STAGE_CALC);
	assert(client->buf_idx >= 0 && client->buf_idx < MAX_CLIENTS);

    // Parse number
    unsigned long long num = strtoull(bufs[client->buf_idx], NULL, 10);

    if (num < 2)
		goto done;

    if (num == 2) {
		is_prime_res = true;
		goto done;
	}

    if (num %2 == 0)
		goto done;

	unsigned long long c = 1;
    for (unsigned long long i = 3; i * i <= num; i += 2, c++) {
        if (num % i == 0)
			goto done;

		if (c == COROUTINE_COUNTER_LIMIT) {
			c = 0;
			aco_yield();
			assert(client->stage == STAGE_CALC);
		}
    }

	is_prime_res = true;

done:
	snprintf(client->response, sizeof(client->response), (is_prime_res) ? "Y\n" : "N\n");
	aco_exit();
}

int main(int argc, char *argv[]) {
    // Parse arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--throughput") == 0) {
            g_mode = MODE_THROUGHPUT;
        } else if (strcmp(argv[i], "--low-latency") == 0) {
            g_mode = MODE_LOW_LATENCY;
        }
    }
    
    printf("Mode: %s\n", g_mode == MODE_THROUGHPUT ? "THROUGHPUT" : "LOW_LATENCY");

	// Initialize client array
	for (int i = 0; i < MAX_CLIENTS; i++)
		clear_client(&clients[i]);
    
    // Setup Unix socket
	server_fd = setup_server_socket();
	if (server_fd < 0)
		exit(1);

    // Initialize io_uring
	struct io_uring_params params;
	memset(&params, 0, sizeof(params));
	if (io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params) < 0) {
		perror("io_uring_queue_init");
		close(server_fd);
		unlink(SOCKET_PATH);
		exit(1);
	}

	// Check if IORING_FEAT_FAST_POLL is supported
	if (!(params.features & IORING_FEAT_FAST_POLL)) {
		perror("IORING_FEAT_FAST_POLL not available in the kernel, quiting...\n");
		close(server_fd);
		unlink(SOCKET_PATH);
		exit(1);
	}

	// Check if buffer selection is supported
	struct io_uring_probe *probe;
	probe = io_uring_get_probe_ring(&ring);
	if (!probe || !io_uring_opcode_supported(probe, IORING_OP_PROVIDE_BUFFERS)) {
		perror("Buffer select not supported, quiting...\n");
		close(server_fd);
		unlink(SOCKET_PATH);
		exit(1);
	}

	io_uring_free_probe(probe);

	// register buffers for buffer selection
	struct io_uring_sqe *sqe;
	struct io_uring_cqe *cqe;

	sqe = io_uring_get_sqe(&ring);
	io_uring_prep_provide_buffers(sqe, bufs, BUFFER_SIZE, MAX_CLIENTS, group_id, 0);

	io_uring_submit(&ring);
	io_uring_wait_cqe(&ring, &cqe);
	if (cqe->res < 0) {
		printf("cqe->res = %d, quiting...\n", cqe->res);
		close(server_fd);
		unlink(SOCKET_PATH);
		exit(1);
	}

	io_uring_cqe_seen(&ring, cqe);

	// Add initial accept request
	add_accept_request();

	// Set up signal handler for graceful shutdown
	signal(SIGINT, server_cleanup_and_exit);
    
    // Initialize libaco
	aco_thread_init(NULL);
	main_co = aco_create(NULL, NULL, 0, NULL, NULL);
    
    // Main event loop
	while (1) {
		handle_requests();
		handle_prime_checks();
	}

	aco_destroy(main_co);
	io_uring_queue_exit(&ring);

    return 0;
}

int
setup_server_socket(void)
{
	int sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (sock_fd < 0) {
		perror("socket");
		return -1;
	}

	unlink(SOCKET_PATH);

	struct sockaddr_un server_addr = {0};
	server_addr.sun_family = AF_UNIX;
	strncpy(server_addr.sun_path, SOCKET_PATH, sizeof(server_addr.sun_path) - 1);

	if (bind(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
		perror("bind");
		close(sock_fd);
		return -1;
	}

	if (listen(sock_fd, 128) < 0) {
		perror("listen");
		close(sock_fd);
		unlink(SOCKET_PATH);
		return -1;
	}

	return sock_fd;
}

client_t*
find_free_client(void)
{
	int i;
	for (i = 0; i < MAX_CLIENTS; i++)
		if (clients[i].stage == STAGE_IDLE)
			return &clients[i];
	return NULL;
}

void
add_accept_request(void)
{
	// prepare accept request
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	io_uring_prep_accept(sqe, server_fd, NULL, NULL, 0);
	io_uring_sqe_set_flags(sqe, 0);

	// prepare connection info
	conn_info conn_i = {
		.client_idx = -1,
		.type = EVENT_TYPE_ACCEPT,
	};
	memcpy(&sqe->user_data, &conn_i, sizeof(conn_i));
	io_uring_submit(&ring);
}

void
add_read_request(client_t *client)
{
	int idx = client - clients;

	assert(client);
	assert(client->stage == STAGE_READ);
	assert(client->fd >= 0);
	assert(client->buffer_len < BUFFER_SIZE);
	assert(idx >= 0 && idx < MAX_CLIENTS);

	// prepare read request
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	io_uring_prep_recv(sqe, client->fd, NULL, BUFFER_SIZE, 0);
	io_uring_sqe_set_flags(sqe, IOSQE_BUFFER_SELECT);
	sqe->buf_group = group_id;


	// prepare connection info
	conn_info conn_i = {
		.client_idx = idx,
		.type = EVENT_TYPE_READ,
	};
	memcpy(&sqe->user_data, &conn_i, sizeof(conn_i));
	io_uring_submit(&ring);
}

void
add_write_request(client_t *client)
{
	int idx = client - clients;

	assert(client);
	assert(client->stage == STAGE_WRITE);
	assert(client->fd >= 0);
	assert(client->buffer_len < BUFFER_SIZE);
	assert(idx >= 0 && idx < MAX_CLIENTS);

	// prepare wirte request
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	io_uring_prep_send(sqe, client->fd, client->response, 3, 0);
	io_uring_sqe_set_flags(sqe, 0);

	// prepare connection info
	conn_info conn_i = {
		.client_idx = idx,
		.type = EVENT_TYPE_WRITE,
	};
	memcpy(&sqe->user_data, &conn_i, sizeof(conn_i));
	io_uring_submit(&ring);
}

void
add_provide_buf(client_t *client)
{
	int bid = client->buf_idx;
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	io_uring_prep_provide_buffers(sqe, bufs[bid], BUFFER_SIZE, 1, group_id, bid);
	conn_info conn_i = {
		.client_idx = client->idx,
		.type = EVENT_TYPE_PROV_BUF,
	};
	memcpy(&sqe->user_data, &conn_i, sizeof(conn_i));
	io_uring_submit(&ring);
}

void
add_close_request(client_t *client)
{
	int idx = client - clients;
	assert(idx >= 0 && idx < MAX_CLIENTS);
	assert(client->stage == STAGE_CLOSE);
	assert(client->fd >= 0);

	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	io_uring_prep_close(sqe, client->fd);

	// prepare connection info
	conn_info conn_i = {
		.client_idx = idx,
		.type = EVENT_TYPE_CLOSE,
	};

	memcpy(&sqe->user_data, &conn_i, sizeof(conn_i));
	io_uring_submit(&ring);
}

void
server_cleanup_and_exit(int sig)
{
	printf("shutting down server - sig = %d\n", sig);
	if (server_fd != -1)
		close(server_fd);

	unlink(SOCKET_PATH);
	io_uring_queue_exit(&ring);
	exit(0);
}

void
handle_requests(void)
{
	struct io_uring_cqe *cqe = NULL;
	unsigned head, count = 0;

	io_uring_for_each_cqe(&ring, head, cqe) {

		count++;
		handle_request(cqe);

		if (count >= REQUESTS_HANDLER_BATCH_SIZE )
			goto done;
	}
done:
	io_uring_cq_advance(&ring, count);
}

void
handle_request(struct io_uring_cqe *cqe)
{
	assert(cqe);
	conn_info conn_i;
	int client_idx = 0, bytes_read = 0;
	client_t *client = NULL;

	memcpy(&conn_i, &cqe->user_data, sizeof(conn_i));
	
	switch (conn_i.type) {

		case EVENT_TYPE_ACCEPT:
			client = find_free_client();

			assert(client);
			assert(client->stage == STAGE_IDLE);
			assert(cqe->res >= 0);

			client->fd = cqe->res;
			client->stage = STAGE_READ;

			// Queue to read from client
			add_read_request(client);

			// Queue to accept more clients
			add_accept_request();
			break;

		case EVENT_TYPE_READ:
			bytes_read = cqe->res;
			client_idx = conn_i.client_idx;

			assert(client_idx >= 0 && client_idx < MAX_CLIENTS);

			client = &clients[client_idx];
			client->buf_idx = cqe->flags >> 16;

			assert(client->buf_idx >= 0 && client->buf_idx < MAX_CLIENTS);
			assert(client->stage == STAGE_READ);

			if (bytes_read <= 0) { /* client disconnected */
				client->stage = STAGE_CLOSE;

				if (cqe->flags & IORING_CQE_F_BUFFER)
					add_provide_buf(client);
				else
					add_close_request(client);

				break;
			}

			// client sent data
			client->buffer_len += bytes_read;

			// check if need to keep reading
			if (bufs[client->buf_idx][client->buffer_len - 1] != '\n') {
				add_read_request(client);
				break;
			}

			// Null-terminate the input string
			bufs[client->buf_idx][client->buffer_len] = '\0';

			// prepare check prime coroutine
			create_coroutine(client);
			client->stage = STAGE_CALC;
			break;

		case EVENT_TYPE_WRITE:
			client_idx = conn_i.client_idx;
			assert(client_idx >= 0 && client_idx < MAX_CLIENTS);
			client = &clients[client_idx];
			assert(client->stage == STAGE_WRITE);

			add_provide_buf(client);

			// prepare for more inputs
			client->stage = STAGE_READ;
			client->buffer_len = 0;
			client->buf_idx = -1;
			assert(client->coro == NULL);
			assert(client->sstk == NULL);
			add_read_request(client);
			break;

		case EVENT_TYPE_PROV_BUF:
			client_idx = conn_i.client_idx;
			assert(client_idx >= 0 && client_idx < MAX_CLIENTS);
			client = &clients[client_idx];
			if (client->stage == STAGE_CLOSE)
				add_close_request(client);
			break;

		case EVENT_TYPE_CLOSE:
			client_idx = conn_i.client_idx;
			assert(client_idx >= 0 && client_idx < MAX_CLIENTS);
			client = &clients[client_idx];
			assert(client->stage == STAGE_CLOSE);
			clear_client(client);

			break;
	}

}

void
handle_prime_checks(void)
{
	static int i = 0;

	for (int c = 0; c < MAX_CLIENTS; c++, i++) {

		if (i == MAX_CLIENTS)
			i = 0;

		if (clients[i].stage != STAGE_CALC)
			continue;

		assert(clients[i].coro);
		assert(clients[i].sstk);
		aco_resume(clients[i].coro);
		assert(clients[i].stage == STAGE_CALC);

		if (clients[i].coro->is_end) {
			// prepare response
			clients[i].stage = STAGE_WRITE;
			aco_destroy(clients[i].coro);
			aco_share_stack_destroy(clients[i].sstk);
			clients[i].coro = NULL;
			clients[i].sstk = NULL;
			add_write_request(&clients[i]);
		}

		return;
	}
}

void
clear_client(client_t *client)
{
	int idx = client - clients;
	assert(idx >= 0 && idx < MAX_CLIENTS);
	/* Client stage */
	client->stage = STAGE_IDLE;

	/* comm data */
	client->fd = 0;
	client->buf_idx = -1;
	client->buffer_len = 0;

	/* coro data */
	// It is the caller reseponsibility to free dyn alloc data
	client->idx = idx;
	client->coro= NULL;
	client->sstk = NULL;
}

void
create_coroutine(client_t *client)
{
	int idx = client - clients;
	assert(idx >= 0 && idx < MAX_CLIENTS);
	client->idx = idx;
	client->sstk = aco_share_stack_new(0);
	client->coro = aco_create(main_co, client->sstk, 0, is_prime, &client->idx);
}
