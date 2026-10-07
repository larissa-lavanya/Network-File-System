// common.h - shared constants, error codes and helper prototypes for NM, SS and client
#ifndef COMMON_H
#define COMMON_H
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <dirent.h>
#include <ftw.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>

#define MAXLINE 8192         // max protocol line length
#define PATHLEN 1024         // max normalized path length
#define FULLLEN 5200         // root (<=4096) + '/' + path
#define CHUNK 4096           // data packet size for read/stream/flush
#define ASYNC_THRESHOLD 4096 // SS writes asynchronously at/above this size (unless --SYNC)
#define BK_ROOT ".backup"    // backup directory kept inside every SS root

// error codes returned to clients (spec 3.3)
enum {
    E_OK             = 0,
    E_NOT_FOUND      = 1,
    E_ALREADY_EXISTS = 2,
    E_FILE_BUSY      = 3,
    E_SS_DOWN        = 4,
    E_INVALID_ARGS   = 5,
    E_INVALID_FORMAT = 6,
    E_PERM           = 7,
    E_IO             = 8,
    E_TIMEOUT        = 9,
    E_NO_SS          = 10,
    E_UNKNOWN_CMD    = 11,
    E_NOT_DIR        = 12,
    E_IS_DIR         = 13
};
const char *err_name(int code);

// growable string buffer
typedef struct {
    char *p;
    size_t n, cap;
} Buf;
void buf_add(Buf *b, const char *s);
void buf_free(Buf *b);

// logging (thread-safe, stdout + optional file)
void log_init(const char *who, const char *file);
void log_msg(const char *fmt, ...);

// networking helpers
int net_listen(int port);                             // listen on all interfaces (port 0 = any)
int net_port(int fd);                                 // local port of a socket
int net_connect(const char *host, int port, int tmo); // connect with timeout (seconds)
void set_timeout(int fd, int sec);                    // recv/send timeout, 0 = none
int send_all(int fd, const void *buf, size_t n);
int recv_all(int fd, void *buf, size_t n);
int send_line(int fd, const char *fmt, ...);
int recv_line(int fd, char *buf, size_t max);
void get_local_ip(char *out, size_t n);
void peer_str(int fd, char *ip, size_t n, int *port);

// path helpers
int norm_path(const char *in, char *out, size_t n); // canonicalize; "." = root; -1 if invalid
int path_is_under(const char *a, const char *b);    // a == b or a inside b
void path_join(const char *a, const char *b, char *out, size_t n);
int mkdir_p(const char *path);
int rm_rf(const char *path);
#endif
