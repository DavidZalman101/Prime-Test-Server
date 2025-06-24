#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>

#define SOCKET_PATH "/tmp/prime_server.sock"
#define BUFFER_SIZE 256

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <number>\n", argv[0]);
        printf("Example: %s 17\n", argv[0]);
        return 1;
    }
    
    // Create socket
    int sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }
    
    // Connect to server
    struct sockaddr_un serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sun_family = AF_UNIX;
    strncpy(serv_addr.sun_path, SOCKET_PATH, sizeof(serv_addr.sun_path) - 1);
    
    if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }
    
    // Send number
    char buffer[BUFFER_SIZE];
    snprintf(buffer, sizeof(buffer), "%s\n", argv[1]);
    
    if (write(sockfd, buffer, strlen(buffer)) < 0) {
        perror("write");
        close(sockfd);
        return 1;
    }
    
    // Read response
    ssize_t n = read(sockfd, buffer, sizeof(buffer) - 1);
    if (n < 0) {
        perror("read");
        close(sockfd);
        return 1;
    }
    
    buffer[n] = '\0';
    printf("Number %s is %sprime\n", argv[1], 
           (buffer[0] == 'Y') ? "" : "not ");
    
    close(sockfd);
    return 0;
}