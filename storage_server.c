//stores files, serves NM commands and client I/O.
// Usage: ./storage_server -n <nm_ip> -p <nm_port> -m <port_for_NM> -c <port_for_clients> -d <root_dir> [-i my_ip] [-e export_path]...
#include "common.h"

static char g_root[4096], g_nm_ip[64], g_my_ip[64]; // 4096 = PATH_MAX required by realpath
static int g_nm_port, g_nm_listen, g_cport, g_delay_us = 2000; // delay per flushed chunk simulates slow disk
static pthread_rwlock_t g_rw[256]; // per-path-bucket reader/writer locks

// helpers 
static pthread_rwlock_t* rw_for(const char (*p))
{
    unsigned h = 5381;
    for (; *p ; p++)
        h = h * 33 + (unsigned char)(*p);
    return &g_rw[h % 256];
}

// validate relative path and build absolute path under root
static int full_path(const char* in, char* full, char* norm)
{
    char n[PATHLEN];
    if(norm_path(in, n, sizeof n))
        return -1;
    if(norm)
        strcpy(norm, n);
    if(!strcmp(n, "."))
        snprintf(full, FULLLEN, "%s", g_root);
    else
        snprintf(full, FULLLEN, "%s/%s", g_root, n);
    return 0;
}

static void err(int fd, int code, const char* msg)
{ 
    send_line(fd, "ERR %d %s", code, msg);
    return;
}

// registration with NM (spec 1.1) 
static void scan(const char* rel, Buf* b, int* cnt)// recursively list files/dirs as "T path" lines
{ 
    char full[FULLLEN];
    full_path(rel, full, NULL);
    DIR* d = opendir(full);
    if(!d)
        return;
    
    struct dirent* e;
    while((e = readdir(d)))
    {
        if(!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;

        if(!strcmp(rel, ".") && !strcmp(e->d_name, BK_ROOT))
            continue; // backups are not client-visible

        int sp = 0;
        char* c;
        for (c=e->d_name ; *c ; c++)
        {
            if(isspace((unsigned char)(*c)))
                sp = 1;
        }
        if(sp)
            continue;
        
        char r[PATHLEN], f[FULLLEN], line[PATHLEN + 8];
        struct stat st;
        path_join(rel, e->d_name, r, sizeof r);
        snprintf(f, sizeof f, "%s/%s", g_root, r);
        if(lstat(f, &st))
            continue;
        
        if(S_ISDIR(st.st_mode))
        {
            snprintf(line, sizeof line, "D %s\n", r);
            buf_add(b, line);
            (*cnt)++;
            scan(r, b, cnt);
        }
        else if(S_ISREG(st.st_mode))
        {
            snprintf(line, sizeof line, "F %s\n", r);
            buf_add(b, line);
            (*cnt)++;
        }
    }

    closedir(d);
    return;
}

static int register_with_nm(char exports[][PATHLEN], int nexp)
{
    Buf b = {0};
    int cnt = 0;
    int i;
    for(i=0 ; i<nexp ; i++) // accessible paths chosen via -e (default: whole root)
    {
        char n[PATHLEN], f[FULLLEN], line[PATHLEN + 8];
        struct stat st;
        if(full_path(exports[i], f, n) || stat(f, &st))
        {
            fprintf(stderr, "export %s invalid, skipped\n", exports[i]);
            continue;
        }

        if(strcmp(n, "."))
        {
            char type;
            if(S_ISDIR(st.st_mode))
                type = 'D';
            else
                type = 'F';

            snprintf(line, sizeof(line), "%c %s\n", type, n);
            buf_add(&b, line);
            cnt++;
        }

        if(S_ISDIR(st.st_mode))
            scan(n, &b, &cnt);
    }

    int fd = -1;
    int tries;
    for(tries=0 ; tries<10 && fd<0 ; tries++)
    {
        fd = net_connect(g_nm_ip, g_nm_port, 3);
        if(fd < 0)
            sleep(1);
    }

    if(fd < 0)
    {
        buf_free(&b);
        return -1;
    }

    send_line(fd, "SS_REG %s %d %d %d", g_my_ip, g_nm_listen, g_cport, cnt);
    if(b.n)
        send_all(fd, b.p, b.n);
    buf_free(&b);

    char r[256];
    int ok = recv_line(fd, r, sizeof r) > 0 && !strncmp(r, "OK", 2);
    log_msg("registration %s: %s (%d paths)", ok ? "succeeded" : "FAILED", r, cnt);

    close(fd);

    if(ok)
        return 0;
    return -1;
}

// tell NM a write finished (spec 3.1: SS informs client via NM)
static void notify_nm(const char* tok, int ok, int async, const char* path)
{
    int fd = net_connect(g_nm_ip, g_nm_port, 3);
    char r[64];
    if(fd < 0)
    {
        log_msg("cannot notify NM of write completion");
        return;
    }

    const char* status;
    if(ok)
        status = "OK";
    else
        status = "FAIL";

    send_line(fd, "WRITE_DONE %s %s %d %s", tok, status, async, path);
    recv_line(fd, r, sizeof r);

    close(fd);
    return;
}

// tree transfer protocol (COPYSRC stream)
static int send_file(int fd, const char* rel, const char* full) // "F rel size" + bytes
{
    struct stat st;
    int f = open(full, O_RDONLY);
    if(f < 0)
        return -1;

    if(fstat(f, &st) < 0)
    {
        close(f);
        return -1;
    }

    send_line(fd, "F %s %lld", rel, (long long)st.st_size);

    char buf[CHUNK];
    long long left = st.st_size;
    while(left > 0)
    {
        size_t chunk_size;
        if(left > CHUNK)
            chunk_size = CHUNK;
        else
            chunk_size = (size_t)left;

        ssize_t r = read(f, buf, chunk_size);
        if(r <= 0 || send_all(fd, buf, r))
        {
            close(f);
            return -1;
        }

        left -= r;
    }

    close(f);
    return 0;
}

static int send_tree(int fd, const char* rel, const char* full, int top)
{
    struct stat st;
    if(lstat(full, &st))
        return -1;

    if(S_ISREG(st.st_mode))
        return send_file(fd, rel, full);

    if(!S_ISDIR(st.st_mode))
        return 0;

    send_line(fd, "D %s", rel);

    DIR* d = opendir(full);
    if(!d)
        return -1;

    struct dirent* e;
    while((e = readdir(d)))
    {
        if(!strcmp(e->d_name, ".") || !strcmp(e->d_name, ".."))
            continue;

        if(top && !strcmp(e->d_name, BK_ROOT))
            continue;

        char r[PATHLEN], f[FULLLEN];
        path_join(rel, e->d_name, r, sizeof r);
        snprintf(f, sizeof f, "%s/%s", full, e->d_name);

        if(send_tree(fd, r, f, 0))
        {
            closedir(d);
            return -1;
        }
    }

    closedir(d);
    return 0;
}

// client port handlers (spec 1.2 client interactions)
static void c_read(int fd, const char* pin) // READ and STREAM: DATA packets then STOP
{
    char full[FULLLEN], n[PATHLEN];
    struct stat st;
    if (full_path(pin, full, n))
    {
        err(fd, E_INVALID_ARGS, "bad path");
        return;
    }

    pthread_rwlock_t* l = rw_for(n);

    pthread_rwlock_rdlock(l);

    int f = open(full, O_RDONLY);
    if(f < 0 || fstat(f, &st) || !S_ISREG(st.st_mode))
    {
        if(f >= 0)
            close(f);
        pthread_rwlock_unlock(l);
        err(fd, E_NOT_FOUND, "cannot open file");
        return;
    }

    char buf[CHUNK];
    ssize_t r;
    while((r = read(f, buf, sizeof buf)) > 0)
    {
        if(send_line(fd, "DATA %zd", r) || send_all(fd, buf, r))
            break;
    }

    send_line(fd, "STOP");
    close(f);

    pthread_rwlock_unlock(l);

    log_msg("READ %s served", n);

    return;
}

static void c_info(int fd, const char* pin) // size, permissions, timestamps
{
    char full[FULLLEN];
    struct stat st;
    if(full_path(pin, full, NULL) || stat(full, &st))
    {
        err(fd, E_NOT_FOUND, "no such path");
        return;
    }

    char perm[10]   = "---------", mt[32], at[32];
    const char* rwx = "rwxrwxrwx";
    int i;
    for(i=0 ; i<9 ; i++)
    {
        if(st.st_mode & (1 << (8 - i)))
            perm[i] = rwx[i];
    }

    struct tm tm;
    localtime_r(&st.st_mtime, &tm);
    strftime(mt, sizeof mt, "%Y-%m-%d %H:%M:%S", &tm);

    localtime_r(&st.st_atime, &tm);
    strftime(at, sizeof at, "%Y-%m-%d %H:%M:%S", &tm);

    send_line(fd, "OK type=%s size=%lld perms=%s (%04o) modified=%s accessed=%s", S_ISDIR(st.st_mode) ? "dir" : "file",
              (long long)st.st_size, perm, st.st_mode & 07777, mt, at);

    return;
}

typedef struct
{
    char path[PATHLEN];
    char tok[32];
    char* data;
    size_t len;
} Flush;

static int write_file(const char* path, const char* data, size_t len, int chunked) // flush data in fixed-size chunks
{
    char full[FULLLEN], n[PATHLEN];
    if (full_path(path, full, n))
        return -1;
    
    pthread_rwlock_t* l = rw_for(n);

    pthread_rwlock_wrlock(l);

    int f = open(full, O_WRONLY | O_TRUNC);
    int rc = 0;
    if(f < 0)
        rc = -1;
    else
    {
        size_t off;
        for(off=0 ; off<len && rc==0 ; )
        {
            size_t k;
            if(len - off > CHUNK)
                k = CHUNK;
            else
                k = len - off;

            if (write(f, data + off, k) != (ssize_t)k)
                rc = -1;
            off += k;
            if (chunked && g_delay_us)
                usleep(g_delay_us);
        }
        fsync(f);
        close(f);
    }

    pthread_rwlock_unlock(l);
    return rc;
}

static void* flush_thread(void* a) // asynchronous write worker (spec 3.1)
{
    Flush* fl = a;
    int rc = write_file(fl->path, fl->data, fl->len, 1);
    const char* status;
    if(rc)
        status = "FAILED";
    else
        status = "flushed";
    
    log_msg("ASYNC WRITE %s %s (%zu bytes)", fl->path, status, fl->len);
    notify_nm(fl->tok, rc == 0, 1, fl->path);
    free(fl->data);
    free(fl);
    return NULL;
}

static void c_write(int fd, const char* line)
{
    char p[PATHLEN], tok[32];
    int sync;
    size_t len;
    if(sscanf(line, "WRITE %1023s %d %31s %zu", p, &sync, tok, &len) != 4)
    {
        err(fd, E_INVALID_ARGS, "usage");
        return;
    }

    char full[FULLLEN], n[PATHLEN];
    struct stat st;
    if(full_path(p, full, n) || stat(full, &st) || !S_ISREG(st.st_mode))
    {
        err(fd, E_NOT_FOUND, "file does not exist");
        notify_nm(tok, 0, 0, p);
        return;
    }

    char* data;
    if (len)
        data = malloc(len);
    else
        data = malloc(1);
    
    set_timeout(fd, 30);

    if(!data || recv_all(fd, data, len))
    {
        free(data);
        err(fd, E_IO, "incomplete data");
        notify_nm(tok, 0, 0, n);
        return;
    }

    if(sync || len < ASYNC_THRESHOLD) // small or --SYNC: write now, ack after persisted
    {
        int rc = write_file(n, data, len, 0);
        free(data);

        const char* status;
        if (rc)
            status = "FAILED";
        else
            status = "ok";
        log_msg("SYNC WRITE %s %zu bytes %s", n, len, status);

        notify_nm(tok, rc == 0, 0, n); // release NM lock before acking the client
        if(rc)
            err(fd, E_IO, "write failed");
        else
            send_line(fd, "OK written %zu bytes (sync)", len);
    }
    else // large: buffer in memory, immediate ACK, flush later
    {
        send_line(fd, "ACK_ASYNC accepted %zu bytes", len);

        Flush* fl = malloc(sizeof *fl);
        snprintf(fl->path, sizeof fl->path, "%s", n);
        snprintf(fl->tok, sizeof fl->tok, "%s", tok);
        fl->data = data;
        fl->len  = len;

        pthread_t t;
        pthread_create(&t, NULL, flush_thread, fl);
        pthread_detach(t);

        log_msg("ASYNC WRITE %s accepted (%zu bytes)", n, len);
    }

    return;
}

static void* client_conn(void* a)
{
    int fd = (int)(long)a;
    char line[MAXLINE], cmd[16], p[PATHLEN];

    set_timeout(fd, 30);

    if(recv_line(fd, line, sizeof line) > 0 && sscanf(line, "%15s %1023s", cmd, p) >= 1)
    {
        if(!strcmp(cmd, "READ"))
            c_read(fd, p);
        else if(!strcmp(cmd, "INFO"))
            c_info(fd, p);
        else if(!strcmp(cmd, "WRITE"))
            c_write(fd, line);
        else if(!strcmp(cmd, "COPYSRC")) // source side of SS->SS copy
        {
            char full[FULLLEN], n[PATHLEN];
            if(full_path(p, full, n))
                err(fd, E_INVALID_ARGS, "bad path");
            else if(send_tree(fd, ".", full, !strcmp(n, ".")) == 0)
                send_line(fd, "END");
            else
                err(fd, E_NOT_FOUND, "copy source unreadable");
        }
        else
            err(fd, E_UNKNOWN_CMD, "unknown");
    }

    close(fd);
    return NULL;
}

// NM port handlers (spec 1.2 commands issued by NM)
static void nm_create(int fd, const char* p, char type)
{
    char full[FULLLEN];
    if(full_path(p, full, NULL) || !strcmp(p, "."))
    {
        err(fd, E_INVALID_ARGS, "bad path");
        return;
    }

    struct stat st;
    if(lstat(full, &st) == 0)
    {
        err(fd, E_ALREADY_EXISTS, "exists");
        return;
    }

    char par[FULLLEN];
    snprintf(par, sizeof par, "%s", full);
    char* sl = strrchr(par, '/');
    if(sl)
    {
        *sl = 0;
        mkdir_p(par);
    }

    int rc;
    if(type == 'D')
        rc = mkdir(full, 0755);
    else
    {
        int f = open(full, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if(f < 0)
            rc = -1;
        else
            rc = close(f);
    }

    if(rc)
        err(fd, E_IO, strerror(errno));
    else
        send_line(fd, "OK created");

    const char* status;
    if (rc)
        status = "FAILED";
    else
        status = "ok";

    log_msg("CREATE %s (%c) %s", p, type, status);

    return;
}

static void nm_delete(int fd, const char* p)
{
    char full[FULLLEN], n[PATHLEN];
    if(full_path(p, full, n) || !strcmp(n, "."))
    {
        err(fd, E_INVALID_ARGS, "bad path");
        return;
    }

    struct stat st;
    if(lstat(full, &st))
    {
        err(fd, E_NOT_FOUND, "no such path");
        return;
    }

    pthread_rwlock_t* l = rw_for(n);

    pthread_rwlock_wrlock(l);
    int rc = rm_rf(full);
    pthread_rwlock_unlock(l);

    if(rc)
        err(fd, E_IO, "delete failed");
    else
        send_line(fd, "OK deleted");

    log_msg("DELETE %s %s", p, rc ? "FAILED" : "ok");

    return;
}

// COPYFROM ip port src dst mode: pull a tree from another SS (mode 0 fail if exists, 1 overwrite, 2 skip existing)
static void nm_copyfrom(int fd, const char* ip, int port, const char* src, const char* dst, int mode)
{
    char dn[PATHLEN], dfull[FULLLEN];
    struct stat st;
    if(full_path(dst, dfull, dn))
    {
        err(fd, E_INVALID_ARGS, "bad destination");
        return;
    }

    if(mode == 0 && lstat(dfull, &st) == 0)
    {
        err(fd, E_ALREADY_EXISTS, "destination exists");
        return;
    }

    int s = net_connect(ip, port, 5);
    if(s < 0)
    {
        err(fd, E_SS_DOWN, "source SS unreachable");
        return;
    }

    set_timeout(s, 30);

    send_line(s, "COPYSRC %s", src);
    Buf made = {0};
    int cnt = 0, fail = 0;
    char line[MAXLINE];

    while(!fail && recv_line(s, line, sizeof line) >= 0)
    {
        if(!strcmp(line, "END"))
            break;
        
        if(!strncmp(line, "ERR", 3))
        {
            fail = 1;
            break;
        }

        char t, rel[PATHLEN];
        long long size = 0;
        if(sscanf(line, "%c %1023s %lld", &t, rel, &size) < 2)
        {
            fail = 1;
            break;
        }

        char tn[PATHLEN], tf[FULLLEN], nl[PATHLEN + 8];
        char rn[PATHLEN];
        if(norm_path(rel, rn, sizeof rn))
        {
            fail = 1;
            break;
        }

        path_join(dn, rn, tn, sizeof tn);
        snprintf(tf, sizeof tf, "%s/%s", g_root, tn);

        if(!strcmp(tn, "."))
            snprintf(tf, sizeof tf, "%s", g_root);

        if(t == 'D')
        {
            int existed = lstat(tf, &st) == 0;
            mkdir_p(tf);
            if(!existed)
            {
                snprintf(nl, sizeof nl, "D %s\n", tn);
                buf_add(&made, nl);
                cnt++;
            }
        }
        else
        {
            char par[FULLLEN];
            snprintf(par, sizeof par, "%s", tf);

            char* sl = strrchr(par, '/');
            if(sl)
            {
                *sl = 0;
                mkdir_p(par);
            }

            int skip = (mode == 2 && lstat(tf, &st) == 0);
            int existed = lstat(tf, &st) == 0;

            char tmp[FULLLEN + 16];
            snprintf(tmp, sizeof tmp, "%s.nfstmp%lu", tf, (unsigned long)pthread_self());

            int f = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            char buf[CHUNK];
            long long left = size;

            while(left > 0) // always drain the stream even when skipping
            {
                size_t k;
                if(left > CHUNK)
                    k = CHUNK;
                else
                    k = left;
                
                if(recv_all(s, buf, k))
                {
                    fail = 1;
                    break;
                }

                if(f >= 0 && !skip && write(f, buf, k) != (ssize_t)k)
                    fail = 1;
                
                left -= k;
            }
            
            if(f >= 0)
                close(f);

            if (skip || fail)
                unlink(tmp);
            else
            {
                if(rename(tmp, tf))
                    fail = 1;
                else if(!existed)
                {
                    snprintf(nl, sizeof nl, "F %s\n", tn);
                    buf_add(&made, nl);
                    cnt++;
                }
            }
        }
    }

    close(s);

    if(fail)
    {
        err(fd, E_IO, "transfer failed");
        if(mode == 0)
            rm_rf(dfull);
    }
    else
    {
        send_line(fd, "OK %d", cnt);
        if (made.n)
            send_all(fd, made.p, made.n);
    }

    log_msg("COPYFROM %s:%d:%s -> %s mode=%d: %s (%d entries)", ip, port, src, dst, mode, fail ? "FAILED" : "ok", cnt);
    buf_free(&made);

    return;
}

static void* nm_conn(void* a)
{
    int fd = (int)(long)a;
    char line[MAXLINE], cmd[16], a1[PATHLEN], a2[PATHLEN], a3[PATHLEN], a4[PATHLEN];
    int port, mode;

    set_timeout(fd, 30);

    if(recv_line(fd, line, sizeof line) > 0)
    {
        sscanf(line, "%15s", cmd);
        if(!strcmp(cmd, "PING"))
            send_line(fd, "PONG");
        else if(sscanf(line, "CREATE %1023s %1023s", a1, a2) == 2)
            nm_create(fd, a1, a2[0]);
        else if(sscanf(line, "DELETE %1023s", a1) == 1)
            nm_delete(fd, a1);
        else if(sscanf(line, "COPYFROM %1023s %d %1023s %1023s %d", a1, &port, a2, a3, &mode) == 5)
        {
            (void)a4;
            nm_copyfrom(fd, a1, port, a2, a3, mode);
        }
        else
            err(fd, E_UNKNOWN_CMD, "unknown command");
    }

    close(fd);
    return NULL;
}

// generic accept loop: one thread per connection
static void* listener(void* a)
{
    void* (*h)(void *) = ((void **)a)[1];
    int ls = (int)(long)((void **)a)[0];
    for (;;)
    {
        int fd = accept(ls, NULL, NULL);
        if(fd < 0)
            continue;
        
        pthread_t t;
        pthread_create(&t, NULL, h, (void *)(long)fd);
        pthread_detach(t);
    }

    return NULL;
}

int main(int argc, char **argv)
{
    char exports[32][PATHLEN];
    int nexp   = 0, o;
    char* root = NULL;
    g_my_ip[0] = 0;

    while((o = getopt(argc, argv, "n:p:m:c:d:i:e:")) != -1)
    {
        switch(o)
        {
            case 'n':
                snprintf(g_nm_ip, sizeof g_nm_ip, "%s", optarg);
                break;
            case 'p':
                g_nm_port = atoi(optarg);
                break;
            case 'm':
                g_nm_listen = atoi(optarg);
                break;
            case 'c':
                g_cport = atoi(optarg);
                break;
            case 'd':
                root = optarg;
                break;
            case 'i':
                snprintf(g_my_ip, sizeof g_my_ip, "%s", optarg);
                break;
            case 'e':
                if (nexp < 32)
                    snprintf(exports[nexp++], PATHLEN, "%s", optarg);
                break;
            default:
                return 1;
        }
    }

    if(!g_nm_ip[0] || !g_nm_port || !g_nm_listen || !g_cport || !root)
    {
        fprintf(stderr,
                "usage: %s -n <nm_ip> -p <nm_port> -m <nm_listen_port> -c <client_port> -d <root_dir> [-i my_ip] [-e "
                "export_path]...\n",
                argv[0]);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    mkdir_p(root);
    if(!realpath(root, g_root))
    {
        perror("root");
        return 1;
    }

    if(!g_my_ip[0])
        get_local_ip(g_my_ip, sizeof g_my_ip);

    if(!nexp)
        strcpy(exports[nexp++], ".");

    if(getenv("SS_FLUSH_DELAY_US"))
        g_delay_us = atoi(getenv("SS_FLUSH_DELAY_US"));
    
    int i;
    for(i=0 ; i<256 ; i++)
        pthread_rwlock_init(&g_rw[i], NULL);
    
    char lf[64];
    snprintf(lf, sizeof lf, "ss_%d.log", g_cport);
    log_init("SS", lf);

    int l1 = net_listen(g_nm_listen);
    int l2 = net_listen(g_cport); // listeners must be up before NM calls back
    if(l1 < 0 || l2 < 0)
    {
        perror("listen");
        return 1;
    }

    static void *a1[2], *a2[2];
    a1[0] = (void *)(long)(l1);
    a1[1] = (void *)(nm_conn);
    a2[0] = (void *)(long)(l2);
    a2[1] = (void *)(client_conn);

    pthread_t t;
    pthread_create(&t, NULL, listener, a1);
    pthread_detach(t);

    pthread_create(&t, NULL, listener, a2);
    pthread_detach(t);

    log_msg("Storage Server root=%s ip=%s nm_port=%d client_port=%d", g_root, g_my_ip, g_nm_listen, g_cport);
    
    if(register_with_nm(exports, nexp))
    {
        fprintf(stderr, "could not register with NM\n");
        return 1;
    }
    
    pause(); // serve forever
    return 0;
}
