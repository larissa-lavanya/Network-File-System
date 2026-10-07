// logging, socket helpers, path utilities
#include "common.h"

static pthread_mutex_t log_mu = PTHREAD_MUTEX_INITIALIZER;
static FILE *log_fp = NULL;
static char log_who[32] = "";

// error code: readable name
const char* err_name(int c)
{
    static const char* n[] = {"OK",
                              "NOT_FOUND",
                              "ALREADY_EXISTS",
                              "FILE_BUSY",
                              "SS_DOWN",
                              "INVALID_ARGS",
                              "INVALID_FORMAT",
                              "PERMISSION",
                              "IO_ERROR",
                              "TIMEOUT",
                              "NO_STORAGE_SERVER",
                              "UNKNOWN_COMMAND",
                              "NOT_A_DIRECTORY",
                              "IS_A_DIRECTORY"};

    if (c >= 0 && c <= E_IS_DIR)
        return n[c];
    return "UNKNOWN";
}

// growable buffer append
void buf_add(Buf* b, const char* s)
{
    size_t l = strlen(s);
    if (b->n + l + 1 > b->cap)
    {
        b->cap = (b->n + l + 1) * 2;
        b->p   = realloc(b->p, b->cap);
    }
    memcpy(b->p + b->n, s, l + 1);
    b->n += l;

    return;
}

void buf_free(Buf* b)
{
    free(b->p);
    b->p = NULL;
    b->n = 0;
    b->cap = 0;

    return;
}

// logging: timestamped line to stdout and log file
void log_init(const char* who, const char* file)
{
    snprintf(log_who, sizeof log_who, "%s", who);
    if (file)
        log_fp = fopen(file, "a");

    return;
}

void log_msg(const char* fmt, ...)
{
    char ts[32], body[4096];
    time_t t = time(NULL);
    struct tm tm;

    localtime_r(&t, &tm);
    strftime(ts, sizeof ts, "%Y-%m-%d %H:%M:%S", &tm);

    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&log_mu);

    printf("[%s] [%s] %s\n", ts, log_who, body);
    fflush(stdout);

    if(log_fp)
    {
        fprintf(log_fp, "[%s] [%s] %s\n", ts, log_who, body);
        fflush(log_fp);
    }

    pthread_mutex_unlock(&log_mu);
    return;
}

// sockets
int net_listen(int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    if (fd < 0)
        return -1;
    
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);

    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons(port);
    if(bind(fd, (struct sockaddr*)&a, sizeof a) < 0 || listen(fd, 128) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

int net_port(int fd)
{
    struct sockaddr_in a;
    socklen_t l = sizeof(a);
    getsockname(fd, (struct sockaddr*)(&a), &l);
    return ntohs(a.sin_port);
}

void set_timeout(int fd, int sec)
{
    struct timeval tv = {sec, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);

    return;
}

// non-blocking connect with poll() timeout
int net_connect(const char* host, int port, int tmo)
{
    struct addrinfo h;
    struct addrinfo* r;
    char ps[16];
    memset(&h, 0, sizeof h);

    h.ai_family = AF_INET;
    h.ai_socktype = SOCK_STREAM;

    snprintf(ps, sizeof ps, "%d", port);

    if(getaddrinfo(host, ps, &h, &r))
        return -1;

    int fd = socket(r->ai_family, r->ai_socktype, 0);
    if(fd < 0)
    {
        freeaddrinfo(r);
        return -1;
    }

    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    int rc = connect(fd, r->ai_addr, r->ai_addrlen);

    if(rc < 0 && errno == EINPROGRESS)
    {
        struct pollfd p = {fd, POLLOUT, 0};
        int e = 0;
        socklen_t l = sizeof(e);
        if (poll(&p, 1, tmo*1000) <= 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, &e, &l) < 0 || e)
            rc = -1;
        else
            rc = 0;
    }

    freeaddrinfo(r);
    
    if(rc < 0)
    {
        close(fd);
        return -1;
    }

    fcntl(fd, F_SETFL, fl);

    if (tmo > 0)
        set_timeout(fd, 30);
    else
        set_timeout(fd, 0);

    return fd;
}

int send_all(int fd, const void* buf, size_t n)
{
    const char* p = buf;
    while(n)
    {
        ssize_t w = send(fd, p, n, MSG_NOSIGNAL);
        if(w <= 0)
        {
            if(w < 0 && errno == EINTR)
                continue;
            return -1;
        }
        p += w;
        n -= w;
    }

    return 0;
}

int recv_all(int fd, void* buf, size_t n)
{
    char* p = buf;
    while(n)
    {
        ssize_t r = recv(fd, p, n, 0);
        if(r <= 0)
        {
            if(r < 0 && errno == EINTR)
                continue;
            return -1;
        }
        p += r;
        n -= r;
    }

    return 0;
}

int send_line(int fd, const char* fmt, ...)
{
    char b[MAXLINE];
    va_list ap;
    va_start(ap, fmt);

    int n = vsnprintf(b, sizeof(b) - 1, fmt, ap);
    va_end(ap);

    if(n < 0)
        return -1;
    if(n > (int)(sizeof(b)) - 2)
        n = sizeof(b) - 2;
    b[n++] = '\n';

    return send_all(fd, b, n);
}

// byte-wise so binary payload after the line is never over-read
int recv_line(int fd, char* buf, size_t max)
{
    size_t n = 0;
    char c;
    while(n + 1 < max)
    {
        ssize_t r = recv(fd, &c, 1, 0);
        if(r <= 0)
        {
            if(r < 0 && errno == EINTR)
                continue;
            return -1;
        }
        if(c == '\n')
            break;
        if(c != '\r')
            buf[n++] = c;
    }

    buf[n] = 0;

    return (int)(n);
}

// local IP via routing-table trick (no packet sent); falls back to loopback
void get_local_ip(char* out, size_t n)
{
    snprintf(out, n, "127.0.0.1");
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return;
    
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);

    a.sin_family = AF_INET;
    a.sin_port = htons(53);

    inet_pton(AF_INET, "8.8.8.8", &a.sin_addr);

    if(connect(fd, (struct sockaddr*)(&a), sizeof (a)) == 0)
    {
        struct sockaddr_in l;
        socklen_t ll = sizeof l;

        if (getsockname(fd, (struct sockaddr*)(&l), &ll) == 0)
            inet_ntop(AF_INET, &l.sin_addr, out, n);
    }

    close(fd);
    return;
}

void peer_str(int fd, char* ip, size_t n, int* port)
{
    struct sockaddr_in a;
    socklen_t l = sizeof a;
    snprintf(ip, n, "?");
    *port = 0;
    if (getpeername(fd, (struct sockaddr*)(&a), &l) == 0)
    {
        inet_ntop(AF_INET, &a.sin_addr, ip, n);
        *port = ntohs(a.sin_port);
    }

    return;
}

// paths
// canonical form: no leading/trailing '/', no '.', '..' rejected, no whitespace; root is "."
int norm_path(const char* in, char* out, size_t n)
{
    char tmp[PATHLEN], *sp, *tok;
    size_t len = 0;
    if (strlen(in) >= PATHLEN)
        return -1;
    
    strcpy(tmp, in);
    out[0] = 0;
    for(tok = strtok_r(tmp, "/", &sp) ; tok ; tok = strtok_r(NULL, "/", &sp))
    {
        if(!strcmp(tok, "."))
            continue;
        if(!strcmp(tok, ".."))
            return -1;

        char* c;
        for(c=tok ; *c ; c++)
        {
            if (isspace((unsigned char)(*c)) || iscntrl((unsigned char)(*c)))
                return -1;
        }

        size_t tl = strlen(tok);
        if(len + tl + 2 >= n)
            return -1;
    
        if(len)
            out[len++] = '/';
        memcpy(out + len, tok, tl);
        len += tl;
        out[len] = 0;
    }

    if(!len)
        snprintf(out, n, ".");
    
    return 0;
}

int path_is_under(const char* a, const char* b)
{
    if (!strcmp(b, ".") || !strcmp(a, b))
        return 1;
    
    size_t l = strlen(b);
    return strncmp(a, b, l) == 0 && a[l] == '/';
}

void path_join(const char* a, const char* b, char* out, size_t n)
{
    if(!strcmp(a, "."))
        snprintf(out, n, "%s", b);
    else if(!strcmp(b, "."))
        snprintf(out, n, "%s", a);
    else
        snprintf(out, n, "%s/%s", a, b);

    return;
}

int mkdir_p(const char* p)
{
    char t[FULLLEN];
    snprintf(t, sizeof t, "%s", p);

    char* c;
    for (c=t+1 ; *c ; c++)
    {
        if(*c == '/')
        {
            *c = 0;
            mkdir(t, 0755);
            *c = '/';
        }
    }

    if (mkdir(t, 0755) == 0 || errno == EEXIST)
        return 0;
    return -1;
}

static int rm_cb(const char* p, const struct stat* s, int f, struct FTW* w)
{
    (void)(s);
    (void)(f);
    (void)(w);
    return remove(p);
}

int rm_rf(const char* p)
{
    return nftw(p, rm_cb, 16, FTW_DEPTH | FTW_PHYS);
}
