CC = gcc
CFLAGS = -Wall -Wextra -O2 -g -D ACO_CONFIG_SHARE_FPU_MXCSR_ENV -Wno-type-limits
LDFLAGS = -luring -lm -Wl,--no-warn-execstack

# Check if libaco files exist
LIBACO_FILES = aco.h aco.c acosw.S aco_assert_override.h
LIBACO_EXISTS = $(shell if [ -f aco.h ] && [ -f aco.c ] && [ -f acosw.S ] && [ -f aco_assert_override.h ]; then echo 1; else echo 0; fi)

all: libaco prime_server test_client benchmark_client

# Download libaco if needed
libaco:
ifeq ($(LIBACO_EXISTS),0)
	@echo "Downloading libaco..."
	@wget -q https://raw.githubusercontent.com/hnes/libaco/master/aco.h
	@wget -q https://raw.githubusercontent.com/hnes/libaco/master/aco.c
	@wget -q https://raw.githubusercontent.com/hnes/libaco/master/acosw.S
	@wget -q https://raw.githubusercontent.com/hnes/libaco/master/aco_assert_override.h
endif

prime_server: libaco prime_server.o aco.o acosw.o
	$(CC) $(CFLAGS) prime_server.o aco.o acosw.o -o $@ $(LDFLAGS)

test_client: test_client.c
	$(CC) $(CFLAGS) $< -o $@

benchmark_client: benchmark_client.c
	$(CC) $(CFLAGS) $< -o $@ -lpthread -lm

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

acosw.o: acosw.S
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o prime_server test_client benchmark_client
	rm -f /tmp/prime_server.sock

.PHONY: all clean libaco
