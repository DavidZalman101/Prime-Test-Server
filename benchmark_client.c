#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/time.h>
#include <time.h>
#include <errno.h>
#include <stdbool.h>
#include <math.h>
#include <limits.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <signal.h>
#include <x86intrin.h>  // For RDTSC intrinsics

#define SOCKET_PATH "/tmp/prime_server.sock"
#define BUFFER_SIZE 256
#define MAX_RESULTS 100000
#define MAX_CONCURRENT 1000
#define MAX_EVENTS 100
#define DEFAULT_TIMEOUT_MS 5000
#define MAX_RETRIES 3
#define RETRY_DELAY_MS 100

// Test numbers: mix of small/large, prime/non-prime
typedef struct {
    const char *number;
    bool is_prime;
    const char *description;
} test_number_t;

test_number_t test_numbers[] = {
    // Small primes
    {"2", true, "smallest prime"},
    {"17", true, "small prime"},
    {"97", true, "small prime"},
    {"193", true, "small prime"},
    {"397", true, "small prime"},
    
    // Small non-primes
    {"1", false, "not prime"},
    {"4", false, "small composite"},
    {"100", false, "small composite"},
    {"200", false, "small composite"},
    {"500", false, "small composite"},
    
    // Medium primes
    {"7919", true, "1000th prime"},
    {"17389", true, "2000th prime"},
    {"27449", true, "3000th prime"},
    
    // Medium non-primes
    {"10000", false, "medium composite"},
    {"25000", false, "medium composite"},
    {"50000", false, "medium composite"},
    
    // Large primes (these will be slow with naive algorithm)
    {"1000003", true, "large prime"},
    {"10000019", true, "large prime"},
    {"100000007", true, "large prime"},
    {"1000000007", true, "very large prime"},
    {"2147483647", true, "Mersenne prime (2^31-1)"},
    
    // Large non-primes
    {"1000000", false, "large composite"},
    {"10000000", false, "large composite"},
    {"100000000", false, "large composite"},
    {"1000000000", false, "very large composite"},
    {"2147483646", false, "large composite"},
    
    // Edge cases
    {"0", false, "zero"},
    {"1", false, "one"},
    {"-5", false, "negative"},
};

#define NUM_TEST_NUMBERS (sizeof(test_numbers) / sizeof(test_numbers[0]))

// Request states
typedef enum {
    REQ_STATE_CONNECTING,
    REQ_STATE_SENDING,
    REQ_STATE_RECEIVING,
    REQ_STATE_COMPLETED,
    REQ_STATE_ERROR
} request_state_t;

// Outstanding request structure
typedef struct {
    int sockfd;
    request_state_t state;
    double start_time;
    char send_buffer[BUFFER_SIZE];
    char recv_buffer[BUFFER_SIZE];
    size_t send_pos;
    size_t recv_pos;
    int test_idx;
    bool in_use;
} outstanding_request_t;

// Result structure for timing
typedef struct {
    double latency_us;  // Changed from latency_ms to latency_us
    const char *number;
    bool correct;
} result_t;

// Global flag for graceful shutdown
volatile bool shutdown_requested = false;

// Global CPU frequency for RDTSC normalization
static double cpu_freq_ghz = 0.0;

// Get CPU frequency from /proc/cpuinfo
double get_cpu_frequency() {
    FILE *fp = fopen("/proc/cpuinfo", "r");
    if (!fp) {
        perror("fopen /proc/cpuinfo");
        return 0.0;
    }
    
    char line[256];
    double freq = 0.0;
    
    while (fgets(line, sizeof(line), fp)) {
        if (strncmp(line, "cpu MHz", 7) == 0) {
            char *colon = strchr(line, ':');
            if (colon) {
                freq = atof(colon + 1) / 1000.0; // Convert MHz to GHz
                break;
            }
        }
    }
    
    fclose(fp);
    
    if (freq == 0.0) {
        // Fallback: try to estimate using rdtsc over a known time period
        printf("Warning: Could not read CPU frequency from /proc/cpuinfo, estimating...\n");
        
        struct timespec start_ts, end_ts;
        uint64_t start_tsc, end_tsc;
        
        clock_gettime(CLOCK_MONOTONIC, &start_ts);
        start_tsc = __rdtsc();
        
        usleep(100000); // Sleep for 100ms
        
        end_tsc = __rdtsc();
        clock_gettime(CLOCK_MONOTONIC, &end_ts);
        
        double elapsed_ns = (end_ts.tv_sec - start_ts.tv_sec) * 1e9 + 
                           (end_ts.tv_nsec - start_ts.tv_nsec);
        double elapsed_cycles = end_tsc - start_tsc;
        
        freq = elapsed_cycles / elapsed_ns; // GHz
    }
    
    return freq;
}

// Get current time using RDTSCP (with fallback to RDTSC)
static inline uint64_t get_cycles() {
    uint32_t aux;
    // Try RDTSCP first (serializing and provides processor ID)
    return __rdtscp(&aux);
}

// Convert cycles to microseconds
double cycles_to_us(uint64_t cycles) {
    if (cpu_freq_ghz == 0.0) {
        return 0.0;
    }
    return (double)cycles / (cpu_freq_ghz * 1e3); // cycles / (GHz * 1M cycles/sec * 1e-6 sec/us)
}

// Get current time in microseconds using RDTSC
double get_time_us() {
    return cycles_to_us(get_cycles());
}

// Set socket to non-blocking mode
bool set_nonblocking(int sockfd) {
    int flags = fcntl(sockfd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return false;
    }
    if (fcntl(sockfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL");
        return false;
    }
    return true;
}

// Send request and measure latency
bool send_request(const char *number, double *latency_us, bool *is_prime_result, bool verbose) {
    int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (verbose) perror("socket");
        return false;
    }
    
    struct sockaddr_un serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, SOCKET_PATH, sizeof(serv_addr.sun_path) - 1);
    
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        if (verbose) perror("connect");
        close(sockfd);
        return false;
    }
    
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "%s\n", number);
    
    double start_time = get_time_us();
    
    if (write(sockfd, buffer, strlen(buffer)) < 0) {
        if (verbose) perror("write");
        close(sockfd);
        return false;
    }
    
    ssize_t n = read(sockfd, buffer, sizeof(buffer) - 1);
    if (n < 0) {
        if (verbose) perror("read");
        close(sockfd);
        return false;
    }
    
    double end_time = get_time_us();
    *latency_us = end_time - start_time;
    
    buffer[n] = '\0';
    *is_prime_result = (buffer[0] == 'Y');
    
    close(sockfd);
    return true;
}

// Check if a test number is valid (numeric and <= max_number)
bool is_valid_test_number(const char *number_str, long long max_number) {
    char *endptr;
    long long number = strtoll(number_str, &endptr, 10);
    
    // Check if conversion was successful (entire string was numeric)
    if (*endptr != '\0') {
        return false; // Invalid number (like "abc")
    }
    
    return number <= max_number;
}

// Signal handler for graceful shutdown
void signal_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        shutdown_requested = true;
        printf("\nShutdown requested, cleaning up...\n");
    }
}

// Test server connectivity
bool test_server_connection(bool verbose) {
    int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) {
        if (verbose) perror("socket");
        return false;
    }
    
    struct sockaddr_un serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, SOCKET_PATH, sizeof(serv_addr.sun_path) - 1);
    
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        if (verbose) {
            printf("Cannot connect to server at %s: %s\n", SOCKET_PATH, strerror(errno));
        }
        close(sockfd);
        return false;
    }
    
    close(sockfd);
    return true;
}

// Initialize a new request with timeout support
bool init_request(outstanding_request_t *req, int test_idx, bool verbose) {
    req->sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (req->sockfd < 0) {
        if (verbose) perror("socket");
        return false;
    }
    
    if (!set_nonblocking(req->sockfd)) {
        close(req->sockfd);
        return false;
    }
    
    req->state = REQ_STATE_CONNECTING;
    req->start_time = get_time_us();
    req->send_pos = 0;
    req->recv_pos = 0;
    req->test_idx = test_idx;
    req->in_use = true;
    
    snprintf(req->send_buffer, sizeof(req->send_buffer), "%s\n", test_numbers[test_idx].number);
    
    struct sockaddr_un serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, SOCKET_PATH, sizeof(serv_addr.sun_path) - 1);
    
    int result = connect(req->sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
    if (result == 0) {
        // Connected immediately
        req->state = REQ_STATE_SENDING;
    } else if (errno == EINPROGRESS) {
        // Connection in progress, will be notified via epoll
        req->state = REQ_STATE_CONNECTING;
    } else {
        if (verbose) perror("connect");
        close(req->sockfd);
        return false;
    }
    
    return true;
}

// Check if request has timed out
bool request_timed_out(outstanding_request_t *req, double timeout_ms) {
    return (get_time_us() - req->start_time) > (timeout_ms * 1000.0); // Convert timeout to microseconds
}

// Handle request state transitions with better error checking
bool handle_request(outstanding_request_t *req, uint32_t events, bool verbose) {
    // Check for socket errors first
    if (events & (EPOLLERR | EPOLLHUP)) {
        if (verbose) {
            int error;
            socklen_t len = sizeof(error);
            if (getsockopt(req->sockfd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error != 0) {
                printf("Socket error for %s: %s\n", test_numbers[req->test_idx].number, strerror(error));
            } else {
                printf("Socket disconnected for %s\n", test_numbers[req->test_idx].number);
            }
        }
        req->state = REQ_STATE_ERROR;
        return false;
    }
    
    switch (req->state) {
        case REQ_STATE_CONNECTING:
            if (events & EPOLLOUT) {
                // Check if connection succeeded
                int error;
                socklen_t len = sizeof(error);
                if (getsockopt(req->sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0) {
                    if (verbose) perror("getsockopt");
                    req->state = REQ_STATE_ERROR;
                    return false;
                }
                if (error != 0) {
                    if (verbose) {
                        printf("Connection failed for %s: %s\n", 
                               test_numbers[req->test_idx].number, strerror(error));
                    }
                    req->state = REQ_STATE_ERROR;
                    return false;
                }
                req->state = REQ_STATE_SENDING;
            }
            break;
            
        case REQ_STATE_SENDING:
            if (events & EPOLLOUT) {
                size_t remaining = strlen(req->send_buffer) - req->send_pos;
                ssize_t sent = write(req->sockfd, req->send_buffer + req->send_pos, remaining);
                if (sent > 0) {
                    req->send_pos += sent;
                    if (req->send_pos >= strlen(req->send_buffer)) {
                        req->state = REQ_STATE_RECEIVING;
                    }
                } else if (sent < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) {
                        // Would block, try again later
                        return true;
                    } else if (errno == EPIPE || errno == ECONNRESET) {
                        if (verbose) printf("Connection lost while sending to %s\n", test_numbers[req->test_idx].number);
                        req->state = REQ_STATE_ERROR;
                        return false;
                    } else {
                        if (verbose) perror("write");
                        req->state = REQ_STATE_ERROR;
                        return false;
                    }
                }
            }
            break;
            
        case REQ_STATE_RECEIVING:
            if (events & EPOLLIN) {
                ssize_t received = read(req->sockfd, req->recv_buffer + req->recv_pos, 
                                      sizeof(req->recv_buffer) - 1 - req->recv_pos);
                if (received > 0) {
                    req->recv_pos += received;
                    req->recv_buffer[req->recv_pos] = '\0';
                    
                    // Check if we have a complete response
                    if (req->recv_pos > 0 && (req->recv_buffer[0] == 'Y' || req->recv_buffer[0] == 'N')) {
                        req->state = REQ_STATE_COMPLETED;
                    }
                } else if (received == 0) {
                    // Connection closed by server
                    if (req->recv_pos == 0) {
                        if (verbose) printf("Server closed connection without response for %s\n", test_numbers[req->test_idx].number);
                        req->state = REQ_STATE_ERROR;
                        return false;
                    }
                    req->state = REQ_STATE_COMPLETED;
                } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    // Would block, try again later
                    return true;
                } else {
                    if (verbose) perror("read");
                    req->state = REQ_STATE_ERROR;
                    return false;
                }
            }
            break;
            
        default:
            break;
    }
    
    return true;
}

// Complete a request and record results
void complete_request(outstanding_request_t *req, result_t *results, int *result_count, bool verbose) {
    double latency_us = get_time_us() - req->start_time;
    bool is_prime_result = (req->recv_buffer[0] == 'Y');
    bool correct = (is_prime_result == test_numbers[req->test_idx].is_prime);
    
    if (*result_count < MAX_RESULTS) {
        results[*result_count].latency_us = latency_us;
        results[*result_count].number = test_numbers[req->test_idx].number;
        results[*result_count].correct = correct;
        (*result_count)++;
    }
    
    if (!correct && verbose) {
        printf("INCORRECT result for %s (expected %s, got %s)\n",
               test_numbers[req->test_idx].number,
               test_numbers[req->test_idx].is_prime ? "prime" : "not prime",
               is_prime_result ? "prime" : "not prime");
    }
    
    close(req->sockfd);
    req->in_use = false;
}

// Cleanup a failed request
void cleanup_request(outstanding_request_t *req) {
    close(req->sockfd);
    req->in_use = false;
}

// Comparison function for qsort
int compare_latencies(const void *a, const void *b) {
    double diff = ((result_t *)a)->latency_us - ((result_t *)b)->latency_us;
    return (diff > 0) - (diff < 0);
}

// Calculate and print percentiles
void print_percentiles(result_t *results, int count) {
    if (count == 0) {
        printf("No results to analyze\n");
        return;
    }
    
    // Sort results by latency
    qsort(results, count, sizeof(result_t), compare_latencies);
    
    printf("\n=== Latency Statistics ===\n");
    printf("Total requests: %d\n", count);
    
    // Calculate percentiles
    int percentiles[] = {50, 90, 95, 99};
    for (int i = 0; i < (int)(sizeof(percentiles) / sizeof(percentiles[0])); i++) {
        int idx = (count * percentiles[i]) / 100;
        if (idx >= count) idx = count - 1;
        printf("p%d: %.2f μs (%s)\n", percentiles[i], 
               results[idx].latency_us, results[idx].number);
    }
    
    // Min and max
    printf("Min: %.2f μs (%s)\n", results[0].latency_us, results[0].number);
    printf("Max: %.2f μs (%s)\n", results[count-1].latency_us, results[count-1].number);
    
    // Average
    double sum = 0;
    for (int i = 0; i < count; i++) {
        sum += results[i].latency_us;
    }
    printf("Average: %.2f μs\n", sum / count);
    
    // Correctness
    int correct_count = 0;
    for (int i = 0; i < count; i++) {
        if (results[i].correct) correct_count++;
    }
    printf("\nCorrectness: %d/%d (%.1f%%)\n", 
           correct_count, count, (correct_count * 100.0) / count);
}

// Print usage
void print_usage(const char *prog_name) {
    printf("Usage: %s [options]\n", prog_name);
    printf("Options:\n");
    printf("  -n <num>     Total number of requests (default: 100)\n");
    printf("  -c <num>     Max concurrent requests (default: 50, max: %d)\n", MAX_CONCURRENT);
    printf("  -m <num>     Maximum number to test (default: no limit)\n");
    printf("  -t <sec>     Request timeout in seconds (default: %.1f)\n", DEFAULT_TIMEOUT_MS / 1000.0);
    printf("  -r           Random order of test numbers\n");
    printf("  -v           Verbose output\n");
    printf("  -l           List test numbers and exit\n");
    printf("  -h           Show this help\n");
    printf("\nExample:\n");
    printf("  %s -n 1000 -c 4 -m 100000 -r   # 4 concurrent requests, 1000 total, max 100000, random order\n", prog_name);
}

// List test numbers
void list_test_numbers() {
    printf("Test numbers:\n");
    printf("%-15s %-10s %s\n", "Number", "Is Prime", "Description");
    printf("--------------------------------------------------------\n");
    for (int i = 0; i < (int)NUM_TEST_NUMBERS; i++) {
        printf("%-15s %-10s %s\n", 
               test_numbers[i].number,
               test_numbers[i].is_prime ? "Yes" : "No",
               test_numbers[i].description);
    }
}

int main(int argc, char *argv[]) {
    // Initialize CPU frequency detection
    cpu_freq_ghz = get_cpu_frequency();
    if (cpu_freq_ghz <= 0.0) {
        fprintf(stderr, "Error: Could not determine CPU frequency\n");
        return 1;
    }
    
    printf("Detected CPU frequency: %.3f GHz\n", cpu_freq_ghz);
    
    int total_requests = 100;
    int max_concurrent = 50;
    long long max_number = LLONG_MAX;
    bool random_order = false;
    bool verbose = false;
    double timeout_ms = DEFAULT_TIMEOUT_MS;
    
    // Install signal handlers
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGPIPE, SIG_IGN); // Ignore broken pipe signals
    
    // Parse command line arguments
    int opt;
    while ((opt = getopt(argc, argv, "n:c:m:t:rvlh")) != -1) {
        switch (opt) {
            case 'n':
                total_requests = atoi(optarg);
                break;
            case 'c':
                max_concurrent = atoi(optarg);
                if (max_concurrent > MAX_CONCURRENT) {
                    printf("Warning: max concurrent limited to %d\n", MAX_CONCURRENT);
                    max_concurrent = MAX_CONCURRENT;
                }
                break;
            case 'm':
                max_number = strtoll(optarg, NULL, 10);
                break;
            case 't':
                timeout_ms = atof(optarg) * 1000.0; // Convert seconds to milliseconds
                break;
            case 'r':
                random_order = true;
                break;
            case 'v':
                verbose = true;
                break;
            case 'l':
                list_test_numbers();
                return 0;
            case 'h':
                printf("Usage: %s [options]\n", argv[0]);
                printf("Options:\n");
                printf("  -n <num>     Total number of requests (default: 100)\n");
                printf("  -c <num>     Max concurrent requests (default: 50, max: %d)\n", MAX_CONCURRENT);
                printf("  -m <num>     Maximum number to test (default: no limit)\n");
                printf("  -t <sec>     Request timeout in seconds (default: %.1f)\n", DEFAULT_TIMEOUT_MS / 1000.0);
                printf("  -r           Random order of test numbers\n");
                printf("  -v           Verbose output\n");
                printf("  -l           List test numbers and exit\n");
                printf("  -h           Show this help\n");
                return 0;
            default:
                printf("Usage: %s [options]\n", argv[0]);
                return 1;
        }
    }
    
    // Validate arguments
    if (total_requests <= 0 || max_concurrent <= 0) {
        fprintf(stderr, "Error: Number of requests and max concurrent must be positive\n");
        return 1;
    }
    
    if (max_number < 0) {
        fprintf(stderr, "Error: Maximum number must be non-negative\n");
        return 1;
    }
    
    if (timeout_ms <= 0) {
        fprintf(stderr, "Error: Timeout must be positive\n");
        return 1;
    }
    
    // Test server connectivity before starting
    printf("Testing server connectivity...\n");
    if (!test_server_connection(verbose)) {
        fprintf(stderr, "Error: Cannot connect to server at %s\n", SOCKET_PATH);
        fprintf(stderr, "Make sure the prime server is running.\n");
        return 1;
    }
    printf("Server connection OK\n");
    
    printf("Starting benchmark...\n");
    printf("Total requests: %d, Max concurrent: %d, Timeout: %.1fs\n", 
           total_requests, max_concurrent, timeout_ms / 1000.0);
    if (max_number != LLONG_MAX) {
        printf("Maximum number limit: %lld\n", max_number);
    }
    if (random_order) printf("Using random order\n");
    
    // Initialize random seed
    srand(time(NULL));
    
    // Allocate results array and request pool
    result_t *results = calloc(MAX_RESULTS, sizeof(result_t));
    outstanding_request_t *requests = calloc(MAX_CONCURRENT, sizeof(outstanding_request_t));
    
    if (!results || !requests) {
        perror("calloc");
        return 1;
    }
    
    // Create epoll instance
    int epfd = epoll_create1(0);
    if (epfd < 0) {
        perror("epoll_create1");
        free(results);
        free(requests);
        return 1;
    }
    
    // Start timing
    double start_time = get_time_us();
    int result_count = 0;
    int requests_sent = 0;
    int active_requests = 0;
    int error_count = 0;
    int timeout_count = 0;
    
    struct epoll_event events[MAX_EVENTS];
    
    // Main event loop
    while (result_count < total_requests && !shutdown_requested) {
        // Start new requests if possible
        while (active_requests < max_concurrent && requests_sent < total_requests && !shutdown_requested) {
            // Find free request slot
            int slot = -1;
            for (int i = 0; i < MAX_CONCURRENT; i++) {
                if (!requests[i].in_use) {
                    slot = i;
                    break;
                }
            }
            if (slot == -1) break; // No free slots
            
            // Select test number
            int test_idx;
            int attempts = 0;
            do {
                if (random_order) {
                    test_idx = rand() % (int)NUM_TEST_NUMBERS;
                } else {
                    test_idx = requests_sent % (int)NUM_TEST_NUMBERS;
                }
                attempts++;
                if (attempts > (int)NUM_TEST_NUMBERS) {
                    printf("No valid test numbers found within limit %lld\n", max_number);
                    goto cleanup;
                }
            } while (!is_valid_test_number(test_numbers[test_idx].number, max_number));
            
            // Initialize request
            if (init_request(&requests[slot], test_idx, verbose)) {
                struct epoll_event ev;
                ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
                ev.data.ptr = &requests[slot];
                
                if (epoll_ctl(epfd, EPOLL_CTL_ADD, requests[slot].sockfd, &ev) < 0) {
                    perror("epoll_ctl ADD");
                    cleanup_request(&requests[slot]);
                    error_count++;
                } else {
                    active_requests++;
                    requests_sent++;
                }
            } else {
                error_count++;
            }
        }
        
        // Check for timeouts
        for (int i = 0; i < MAX_CONCURRENT; i++) {
            if (requests[i].in_use && request_timed_out(&requests[i], timeout_ms)) {
                if (verbose) {
                    printf("Request timeout for %s (%.2fms)\n", 
                           test_numbers[requests[i].test_idx].number, 
                           (get_time_us() - requests[i].start_time) / 1000.0);
                }
                epoll_ctl(epfd, EPOLL_CTL_DEL, requests[i].sockfd, NULL);
                cleanup_request(&requests[i]);
                active_requests--;
                timeout_count++;
            }
        }
        
        // Wait for events with timeout
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, 100); // 100ms timeout
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            error_count++;
            break;
        }
        
        // Process events
        for (int i = 0; i < nfds; i++) {
            outstanding_request_t *req = (outstanding_request_t *)events[i].data.ptr;
            
            if (handle_request(req, events[i].events, verbose)) {
                if (req->state == REQ_STATE_COMPLETED) {
                    epoll_ctl(epfd, EPOLL_CTL_DEL, req->sockfd, NULL);
                    complete_request(req, results, &result_count, verbose);
                    active_requests--;
                }
            } else {
                // Request failed
                epoll_ctl(epfd, EPOLL_CTL_DEL, req->sockfd, NULL);
                cleanup_request(req);
                active_requests--;
                error_count++;
            }
        }
        
        if (verbose && requests_sent % 100 == 0 && requests_sent > 0) {
            printf("Progress: %d/%d requests sent, %d active, %d completed, %d errors, %d timeouts\n",
                   requests_sent, total_requests, active_requests, result_count, error_count, timeout_count);
        }
        
        // Emergency exit if too many errors
        if (error_count > total_requests / 4) {
            fprintf(stderr, "Too many errors (%d), aborting\n", error_count);
            break;
        }
    }

cleanup:
    double end_time = get_time_us();
    double total_time = (end_time - start_time) / 1000000.0; // Convert to seconds
    
    printf("\nBenchmark %s in %.2f seconds\n", 
           shutdown_requested ? "interrupted" : "completed", total_time);
    if (result_count > 0) {
        printf("Throughput: %.2f requests/second\n", result_count / total_time);
    }
    
    if (error_count > 0) {
        printf("Errors: %d\n", error_count);
    }
    if (timeout_count > 0) {
        printf("Timeouts: %d\n", timeout_count);
    }
    
    // Print results
    print_percentiles(results, result_count);
    
    // Cleanup remaining requests
    for (int i = 0; i < MAX_CONCURRENT; i++) {
        if (requests[i].in_use) {
            close(requests[i].sockfd);
        }
    }
    
    close(epfd);
    free(results);
    free(requests);
    
    return (error_count > 0 || timeout_count > 0) ? 1 : 0;
}