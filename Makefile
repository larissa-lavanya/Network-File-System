# Makefile - builds naming_server, storage_server and client
CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread
COMMON = common.o
all: naming_server storage_server client
naming_server: naming_server.o common.o trie.o lru.o
	$(CC) $(CFLAGS) -o $@ $^
storage_server: storage_server.o common.o
	$(CC) $(CFLAGS) -o $@ $^
client: client.o common.o
	$(CC) $(CFLAGS) -o $@ $^
%.o: %.c common.h trie.h lru.h
	$(CC) $(CFLAGS) -c $<
clean:
	rm -f *.o naming_server storage_server client *.log
