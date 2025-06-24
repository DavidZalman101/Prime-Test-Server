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

#include "liburing.h"
#include "aco.h"

#define SOCKET_PATH "/tmp/prime_server.sock"
#define MAX_CLIENTS 128

typedef enum {
    MODE_THROUGHPUT,
    MODE_LOW_LATENCY
} opt_mode_t;

opt_mode_t g_mode = MODE_THROUGHPUT;

// Prime checking - DO NOT CHANGE THE ALGORITHM
// You can add yield points to improve performance
bool is_prime(const char *num_str) {
    // Parse number
    unsigned long long num = strtoull(num_str, NULL, 10);

    if (num < 2) return false;
    if (num == 2) return true;
    if (num % 2 == 0) return false;
    
    for (unsigned long long i = 3; i * i <= num; i += 2) {
        if (num % i == 0) return false;
    }
    
    return true;
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
    
    // TODO: Setup Unix socket
    
    // TODO: Initialize io_uring
    
    // TODO: Initialize libaco
    
    // TODO: Main event loop
    
    return 0;
}