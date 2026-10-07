// directory service, SS registry, client front-end,
// failure detection, replication orchestration, locking, logging
// Usage: ./naming_server -p <port> [-l logfile]
#include "common.h"
#include "trie.h"
#include "lru.h"

#define MAX_SS 64
#define MAX_LOCKS 256
#define HB_INTERVAL 2 // heartbeat period (s)
#define LOCK_TTL 300 // stale write-lock expiry (s)

// global state (all guarded by g_mu)
typedef struct
{
    char ip[64];
    int nm_port;
    int client_port;
    int alive;
    int fails;
    int nb;
    int backup[2];
} SS;

typedef struct
{
    int active;
    int ss;
    int cport;
    char path[PATHLEN];
    char token[32];
    char cip[64];
    time_t t;
} Lock;

typedef struct
{
    int fd;
    int port;
    char ip[64];
    int nport;
} Conn;

static SS g_ss[MAX_SS];
static int g_nss = 0, g_rr = 0;
static Lock g_lk[MAX_LOCKS];
static unsigned long g_tok = 0;
static Trie* g_trie;
static LRU* g_lru;
static pthread_mutex_t g_mu = PTHREAD_MUTEX_INITIALIZER;

// small helpers
// normalize client path and forbid the reserved backup dir
static int np(const char* in, char* out)
{
    if(norm_path(in, out, PATHLEN))
        return -1;
    if(path_is_under(out, BK_ROOT))
        return -1;
    return 0;
}

// send ok/err reply to client and log it (spec 3.7)
static void reply_ok(Conn* c, const char* fmt, ...)
{
    char b[MAXLINE - 8];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    send_line(c->fd, "OK %s", b);
    log_msg("RESP -> %s:%d OK %.200s", c->ip, c->port, b);

    return;
}

static void reply_err(Conn* c, int code, const char* fmt, ...)
{
    char b[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(b, sizeof b, fmt, ap);
    va_end(ap);
    send_line(c->fd, "ERR %d %s", code, b);
    log_msg("RESP -> %s:%d ERR %d (%s) %s", c->ip, c->port, code, err_name(code), b);

    return;
}

// push an async notification to a client's notify port
static void notify_client(const char* ip, int port, const char* msg)
{
    int fd = net_connect(ip, port, 2);
    if(fd < 0)
    {
        log_msg("notify to %s:%d failed", ip, port);
        return;
    }

    send_line(fd, "NOTIFY %s", msg);
    close(fd);
    log_msg("NOTIFY -> %s:%d %s", ip, port, msg);

    return;
}

// path lookup: LRU cache first, then trie (spec 3.4)
static int lookup_locked(const char* p, int* ss, int* isdir)
{
    if(lru_get(g_lru, p, ss, isdir))
    {
        log_msg("LOOKUP %s: cache HIT", p);
        return 1;
    }

    if(trie_find(g_trie, p, ss, isdir))
    {
        lru_put(g_lru, p, *ss, *isdir);
        log_msg("LOOKUP %s: cache MISS, trie hit", p);
        return 1;
    }

    log_msg("LOOKUP %s: not found", p);
    return 0;
}

// round-robin pick among alive storage servers
static int pick_alive_locked(void)
{
    int k;
    for(k=0 ; k<g_nss ; k++)
    {
        int i = (g_rr + k) % g_nss;
        if(g_ss[i].alive)
        {
            g_rr = i + 1;
            return i;
        }
    }

    return -1;
}

// where can this path be read from? primary if alive, else a backup copy (spec 3.5)
// returns 0 primary, 1 backup, -1 none
static int resolve_read_locked(int ss, const char* path, char* ip, int* port, char* actual)
{
    SS* s = &g_ss[ss];
    if(s->alive)
    {
        strcpy(ip, s->ip);
        *port = s->client_port;
        snprintf(actual, FULLLEN, "%s", path);
        return 0;
    }

    int i;
    for(i=0 ; i<s->nb ; i++)
    {
        SS* b = &g_ss[s->backup[i]];
        if(b->alive)
        {
            strcpy(ip, b->ip);
            *port = b->client_port;

            if(!strcmp(path, "."))
                snprintf(actual, FULLLEN, "%s/ss%d", BK_ROOT, ss);
            else
                snprintf(actual, FULLLEN, "%s/ss%d/%s", BK_ROOT, ss, path);
            
            return 1;
        }
    }

    return -1;
}

// write locks (spec 3.2: one writer, readers blocked while writing)
static int path_busy_locked(const char* p)
{
    time_t now = time(NULL);
    int i;
    for(i=0 ; i<MAX_LOCKS ; i++)
    {
        if(!g_lk[i].active)
            continue;

        if(now - g_lk[i].t > LOCK_TTL)
        {
            g_lk[i].active = 0;
            continue;
        }

        if(path_is_under(g_lk[i].path, p) || path_is_under(p, g_lk[i].path))
            return 1;
    }

    return 0;
}

static Lock* lock_acquire_locked(const char* path, int ss, const Conn* c, int notify_port)
{
    int i;
    for(i=0 ; i<MAX_LOCKS ; i++)
    {
        if(!g_lk[i].active)
        {
            Lock* l = &g_lk[i];
            l->active = 1;
            l->ss = ss;
            l->cport = notify_port;
            l->t = time(NULL);

            snprintf(l->path, sizeof l->path, "%s", path);
            snprintf(l->cip, sizeof l->cip, "%s", c->ip);
            snprintf(l->token, sizeof l->token, "T%lu", ++g_tok);

            return l;
        }
    }

    return NULL;
}

static Lock* lock_by_token_locked(const char* tok)
{
    int i;
    for(i=0 ; i<MAX_LOCKS ; i++)
    {
        if(g_lk[i].active && !strcmp(g_lk[i].token, tok))
            return &g_lk[i];
    }

    return NULL;
}

// talking to storage servers
static int ss_open(const SS* s, int tmo)
{ 
    return net_connect(s->ip, s->nm_port, tmo);
}

// one-line request/response to an SS; returns 0 if transport ok
static int ss_rpc(const SS* s, char* resp, size_t n, const char* fmt, ...)
{
    char req[MAXLINE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof req, fmt, ap);
    va_end(ap);

    int fd = ss_open(s, 3);
    if (fd < 0)
        return -1;

    set_timeout(fd, 30);
    send_line(fd, "%s", req);

    int r = recv_line(fd, resp, n);
    close(fd);

    if(r < 0)
        return -1;
    return 0;
}

// replication (spec 3.5): async, NM never waits
typedef struct
{
    int p;
    int b;
    int exists;
    char path[PATHLEN];
} RepJob;

static void do_replicate(int p, int b, const char* path, int exists)
{
    SS sp, sb;
    pthread_mutex_lock(&g_mu);
    sp = g_ss[p];
    sb = g_ss[b];
    pthread_mutex_unlock(&g_mu);
    if(!sb.alive || (exists && !sp.alive))
        return;

    char dst[FULLLEN];
    if(!strcmp(path, "."))
        snprintf(dst, sizeof dst, "%s/ss%d", BK_ROOT, p);
    else
        snprintf(dst, sizeof dst, "%s/ss%d/%s", BK_ROOT, p, path);
        
    int fd = ss_open(&sb, 3);
    if(fd < 0)
        return;
    
    set_timeout(fd, 120);

    char r[MAXLINE];
    if(exists)
        send_line(fd, "COPYFROM %s %d %s %s 1", sp.ip, sp.client_port, path, dst);
    else
        send_line(fd, "DELETE %s", dst);
    
    int n = recv_line(fd, r, sizeof r);
    close(fd);

    if(exists)
    {
        if(n > 0)
            log_msg("REPLICATE SS%d:%s -> SS%d (copy): %s", p, path, b, r);
        else
            log_msg("REPLICATE SS%d:%s -> SS%d (copy): no reply", p, path, b);
    }
    else
    {
        if(n > 0)
            log_msg("REPLICATE SS%d:%s -> SS%d (delete): %s", p, path, b, r);
        else
            log_msg("REPLICATE SS%d:%s -> SS%d (delete): no reply", p, path, b);
    }

    return;
}

static void* rep_thread(void* a)
{
    RepJob* j = a;
    do_replicate(j->p, j->b, j->path, j->exists);
    free(j);
    return NULL;
}

static void spawn_rep(int p, int b, const char* path, int exists)
{
    RepJob* j = malloc(sizeof *j);
    j->p = p;
    j->b = b;
    j->exists = exists;
    snprintf(j->path, sizeof j->path, "%s", path);
    pthread_t t;
    pthread_create(&t, NULL, rep_thread, j);
    pthread_detach(t);

    return;
}

// replicate a changed path of SS p to all of p's backups
static void replicate_async(int p, const char* path, int exists)
{
    int bk[2], nb;
    pthread_mutex_lock(&g_mu);
    nb = g_ss[p].nb;
    bk[0] = g_ss[p].backup[0];
    bk[1] = g_ss[p].backup[1];
    pthread_mutex_unlock(&g_mu);

    int i;
    for (i=0 ; i<nb ; i++)
        spawn_rep(p, bk[i], path, exists);

    return;
}

// heartbeat: failure detection (spec 3.5)
static int ping_ss(const SS* s)
{
    int fd = ss_open(s, 1);
    char r[32];
    if(fd < 0)
        return 0;

    set_timeout(fd, 2);

    send_line(fd, "PING");

    int ok = recv_line(fd, r, sizeof r) > 0 && !strcmp(r, "PONG");
    close(fd);

    return ok;
}

static void* hb_thread(void* a)
{
    (void)a;
    for(;;)
    {
        sleep(HB_INTERVAL);
        pthread_mutex_lock(&g_mu);
        int n = g_nss;
        pthread_mutex_unlock(&g_mu);

        int i;
        for(i=0 ; i<n ; i++)
        {
            SS s;
            pthread_mutex_lock(&g_mu);
            s = g_ss[i];
            pthread_mutex_unlock(&g_mu);

            if(!s.alive)
                continue;

            int ok = ping_ss(&s);

            struct
            {
                char ip[64];
                int port;
                char path[PATHLEN];
            } fl[MAX_LOCKS];

            int nf = 0;
            pthread_mutex_lock(&g_mu);

            if(ok)
                g_ss[i].fails = 0;
            else if(++g_ss[i].fails >= 2)
            {
                g_ss[i].alive = 0;
                log_msg("FAILURE DETECTED: SS%d (%s:%d) is DOWN", i, s.ip, s.client_port);

                int k;
                for(k=0 ; k<MAX_LOCKS ; k++)
                {
                    if(g_lk[k].active && g_lk[k].ss == i) // partial writes (spec 3.1)
                    {
                        snprintf(fl[nf].ip, 64, "%s", g_lk[k].cip);
                        fl[nf].port = g_lk[k].cport;
                        snprintf(fl[nf].path, PATHLEN, "%s", g_lk[k].path);
                        nf++;
                        g_lk[k].active = 0;
                    }
                }
            }

            pthread_mutex_unlock(&g_mu);

            int k;
            for(k=0 ; k<nf ; k++)
            {
                char m[PATHLEN + 80];
                snprintf(m, sizeof m, "WRITE_FAILED %s (storage server went down)", fl[k].path);
                if(fl[k].port > 0)
                    notify_client(fl[k].ip, fl[k].port, m);
            }
        }
    }

    return NULL;
}

// SS registration / recovery (spec 1.1, 1.2, 3.6)
typedef struct
{
    int id;
} RecJob;

// recovery: restore own data from a backup, then refresh backups we hold for others
static void *recover_thread(void* a)
{
    int id = ((RecJob*)(a))->id;
    free(a);

    SS me;
    int bk[2], nb;
    SS bs[2];

    pthread_mutex_lock(&g_mu);

    me = g_ss[id];
    nb = g_ss[id].nb;

    int i;
    for(i=0 ; i<nb ; i++)
    {
        bk[i] = g_ss[id].backup[i];
        bs[i] = g_ss[bk[i]];
    }

    pthread_mutex_unlock(&g_mu);

    char r[MAXLINE];
    for(i=0 ; i<nb ; i++)
    {
        if (bs[i].alive) // mode 2 = copy only missing files, no new NM entries
        {
            if(ss_rpc(&me, r, sizeof r, "COPYFROM %s %d %s/ss%d . 2", bs[i].ip, bs[i].client_port, BK_ROOT, id) == 0)
                log_msg("RECOVERY: SS%d restored from backup SS%d: %s", id, bk[i], r);
            break;
        }
    }

    int p;
    for(p=0 ; p<MAX_SS ; p++) // resync backups hosted on this SS
    {
        int is_bk = 0;
        int alive;

        pthread_mutex_lock(&g_mu);

        if(p < g_nss && p != id)
        {
            alive = g_ss[p].alive;
            for(i=0 ; i<g_ss[p].nb ; i++)
            {
                if (g_ss[p].backup[i] == id)
                    is_bk = 1;
            }
        }
        else
            alive = 0;

        pthread_mutex_unlock(&g_mu);

        if(is_bk && alive)
            do_replicate(p, id, ".", 1);
    }

    return NULL;
}

typedef struct
{
    int p;
    int b;
} SyncJob;

static void* sync_thread(void* a)
{
    SyncJob* j = a;
    do_replicate(j->p, j->b, ".", 1);
    free(j);
    return NULL;
}

static void handle_register(Conn* c, char* line)
{
    char ip[64];
    int nmp, cp, n;
    if(sscanf(line, "SS_REG %63s %d %d %d", ip, &nmp, &cp, &n) != 4)
    {
        send_line(c->fd, "ERR %d bad registration", E_INVALID_ARGS);
        return;
    }

    log_msg("SS_REG from %s:%d: ip=%s nm_port=%d client_port=%d paths=%d", c->ip, c->port, ip, nmp, cp, n);
    char (*ents)[PATHLEN + 4] = malloc((size_t)(n + 1) * (PATHLEN + 4));

    int i;
    for(i=0 ; i<n; i++)
    {
        if(recv_line(c->fd, ents[i], PATHLEN + 4) < 0)
        {
            free(ents);
            return;
        }
    }

    int id = -1, recovered = 0, nsync = 0;
    SyncJob jobs[MAX_SS * 2];

    pthread_mutex_lock(&g_mu);

    for(i=0 ; i<g_nss ; i++)
    {
        if(!strcmp(g_ss[i].ip, ip) && g_ss[i].client_port == cp)
        {
            id = i;
            recovered = 1;
            break;
        }
    }

    if(recovered) // SS came back: no new entries are added (spec 3.6)
    {
        g_ss[id].alive = 1;
        g_ss[id].fails = 0;
        g_ss[id].nm_port = nmp;
    }
    else if(g_nss >= MAX_SS)
    {
        pthread_mutex_unlock(&g_mu);
        free(ents);
        send_line(c->fd, "ERR %d too many storage servers", E_NO_SS);
        return;
    }
    else
    {
        id = g_nss++;
        SS* s = &g_ss[id];
        memset(s, 0, sizeof *s);
        snprintf(s->ip, sizeof s->ip, "%s", ip);

        s->nm_port     = nmp;
        s->client_port = cp;
        s->alive       = 1;

        for(i=0 ; i<n ; i++) // register accessible paths in the trie
        {
            char t = ents[i][0];
            char p[PATHLEN];

            if(norm_path(ents[i] + 2, p, sizeof p) || !strcmp(p, ".") || path_is_under(p, BK_ROOT))
                continue;
            
            if(trie_insert(g_trie, p, id, t == 'D') < 0)
                log_msg("  path %s already owned by another SS, skipped", p);
        }

        int p;
        for(p=0 ; p<g_nss ; p++) // ring backups: next two SS (once >2 SS), 1 if only 2
        {
            int nw[2], nn = 0;
            int k;
            for(k=1 ; k<g_nss && nn<2 ; k++)
                nw[nn++] = (p + k) % g_nss;
            
            for(i=0 ; i<nn ; i++)
            {
                int had = 0;
                int j;
                for(j=0 ; j<g_ss[p].nb ; j++)
                {
                    if(g_ss[p].backup[j] == nw[i])
                        had = 1;
                }

                if(!had)
                {
                    jobs[nsync].p = p;
                    jobs[nsync].b = nw[i];
                    nsync++;
                }
            }

            g_ss[p].nb = nn;

            if(nn > 0)
                g_ss[p].backup[0] = nw[0];
            else
                g_ss[p].backup[0] = -1;

            if(nn > 1)
                g_ss[p].backup[1] = nw[1];
            else
                g_ss[p].backup[1] = -1;
        }
    }

    pthread_mutex_unlock(&g_mu);

    free(ents);
    send_line(c->fd, "OK %d", id);
    log_msg("SS%d %s (%s:%d client port %d)", id, recovered ? "RECOVERED/re-registered" : "REGISTERED", ip, nmp, cp);
    
    pthread_t t;
    if(recovered)
    {
        RecJob* r = malloc(sizeof(*r));
        r->id = id;
        pthread_create(&t, NULL, recover_thread, r);
        pthread_detach(t);
    }

    for(i=0 ; i<nsync ; i++)
    {
        SyncJob *j = malloc(sizeof(*j));
        *j = jobs[i];
        pthread_create(&t, NULL, sync_thread, j);
        pthread_detach(t);
    }

    return;
}

// SS reports write completion (sync or async)
static void handle_write_done(Conn* c, char* line)
{
    char tok[32], st[16], path[PATHLEN];
    int async;
    if (sscanf(line, "WRITE_DONE %31s %15s %d %1023s", tok, st, &async, path) != 4)
        return;
    
    log_msg("ACK from SS %s:%d: WRITE_DONE token=%s status=%s async=%d path=%s", c->ip, c->port, tok, st, async, path);
    
    char cip[64] = "";
    int cport = 0, ss = -1;

    pthread_mutex_lock(&g_mu);

    Lock* l = lock_by_token_locked(tok);
    if(l)
    {
        snprintf(cip, sizeof cip, "%s", l->cip);
        cport = l->cport;
        ss = l->ss;
        l->active = 0;
    }

    pthread_mutex_unlock(&g_mu);

    send_line(c->fd, "OK");

    int ok = !strcmp(st, "OK");
    if(ok && ss >= 0)
        replicate_async(ss, path, 1); // spec 3.5: async duplication of every write

    if(async && cport > 0)
    {
        char m[PATHLEN + 64];
        if(ok)
            snprintf(m, sizeof m, "WRITE_DONE %s (asynchronous write flushed)", path);
        else
            snprintf(m, sizeof m, "WRITE_FAILED %s (storage server write error)", path);
        notify_client(cip, cport, m);
    }

    return;
}

// client command handlers
static int has_audio_ext(const char* p)
{
    static const char* e[] = {".mp3", ".wav", ".flac", ".ogg", ".m4a", ".aac", ".opus"};
    const char* d = strrchr(p, '.');
    if(!d)
        return 0;

    size_t i;
    for(i=0 ; i<(sizeof(e) / sizeof(*e)) ; i++)
    {
        if(!strcasecmp(d, e[i]))
            return 1;
    }

    return 0;
}

// READ / STREAM / INFO: give client the SS address (spec 2 i)
static void h_access(Conn* c, const char* cmd, const char* pin)
{
    char p[PATHLEN], ip[64], act[FULLLEN];
    int ss, isdir, port;
    if(np(pin, p) || !strcmp(p, "."))
    {
        reply_err(c, E_INVALID_ARGS, "invalid path");
        return;
    }

    int info = !strcmp(cmd, "INFO");

    pthread_mutex_lock(&g_mu);

    if(!lookup_locked(p, &ss, &isdir))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_FOUND, "%s does not exist", p);
        return;
    }

    if(!info && isdir)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_IS_DIR, "%s is a directory", p);
        return;
    }

    if(!strcmp(cmd, "STREAM") && !has_audio_ext(p))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_INVALID_FORMAT, "not a supported audio format");
        return;
    }

    if(!info && path_busy_locked(p))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_FILE_BUSY, "%s is being written by another client", p);
        return;
    }

    int rc = resolve_read_locked(ss, p, ip, &port, act);

    pthread_mutex_unlock(&g_mu);

    if(rc < 0)
    {
        reply_err(c, E_SS_DOWN, "storage server down and no backup available");
        return;
    }

    if (rc == 1)
        log_msg("FAILOVER: serving %s from backup", p);

    reply_ok(c, "%s %d %s -", ip, port, act);

    return;
}

// WRITE: grant a lock and give SS address (spec 3.1/3.2)
static void h_write(Conn* c, const char* pin)
{
    char p[PATHLEN];
    int ss, isdir;
    if(np(pin, p) || !strcmp(p, "."))
    {
        reply_err(c, E_INVALID_ARGS, "invalid path");
        return;
    }

    pthread_mutex_lock(&g_mu);

    if(!lookup_locked(p, &ss, &isdir))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_FOUND, "%s does not exist (CREATE it first)", p);
        return;
    }

    if(isdir)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_IS_DIR, "%s is a directory", p);
        return;
    }

    if(path_busy_locked(p))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_FILE_BUSY, "%s is being written by another client", p);
        return;
    }

    if(!g_ss[ss].alive)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_SS_DOWN, "storage server is down (backups are read-only)");
        return;
    }

    Lock* l = lock_acquire_locked(p, ss, c, c->nport);
    if(!l)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_FILE_BUSY, "too many concurrent writes");
        return;
    }

    char ip[64], tok[32];
    int port = g_ss[ss].client_port;
    strcpy(ip, g_ss[ss].ip);
    strcpy(tok, l->token);

    pthread_mutex_unlock(&g_mu);

    reply_ok(c, "%s %d %s %s", ip, port, p, tok);

    return;
}

// CREATE <dir> <name> [-d]: forwarded to the SS owning the parent (spec ex.3)
static void h_create(Conn* c, const char* dir_in, const char* name, int isdir_new)
{
    char par[PATHLEN], full[PATHLEN];
    int pss = -1, pdir = 1;
    if(np(dir_in, par) || strchr(name, '/') || !strcmp(name, ".") || !strcmp(name, "..") || !strcmp(name, BK_ROOT))
    {
        reply_err(c, E_INVALID_ARGS, "invalid path or name");
        return;
    }

    path_join(par, name, full, sizeof full);

    pthread_mutex_lock(&g_mu);

    if(strcmp(par, ".") && !lookup_locked(par, &pss, &pdir))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_FOUND, "parent %s does not exist", par);
        return;
    }

    if(!pdir)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_DIR, "%s is not a directory", par);
        return;
    }

    int d2, s2;
    if(trie_find(g_trie, full, &s2, &d2))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_ALREADY_EXISTS, "%s already exists", full);
        return;
    }

    if(pss < 0)
        pss = pick_alive_locked();
    if(pss < 0)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NO_SS, "no storage server available");
        return;
    }

    if(!g_ss[pss].alive)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_SS_DOWN, "storage server is down");
        return;
    }

    SS s = g_ss[pss];

    pthread_mutex_unlock(&g_mu);

    char r[MAXLINE];
    char type;
    if(isdir_new)
        type = 'D';
    else
        type = 'F';

    if(ss_rpc(&s, r, sizeof r, "CREATE %s %c", full, type) < 0)
    {
        reply_err(c, E_SS_DOWN, "storage server unreachable");
        return;
    }

    log_msg("ACK from SS%d: %s", pss, r);
    if(strncmp(r, "OK", 2))
    {
        int code = E_IO;
        sscanf(r, "ERR %d", &code);
        reply_err(c, code, "storage server refused: %s", r);
        return;
    }
    
    pthread_mutex_lock(&g_mu);
    trie_insert(g_trie, full, pss, isdir_new);
    pthread_mutex_unlock(&g_mu); // dynamic path update

    replicate_async(pss, full, 1);
    reply_ok(c, "created %s on SS%d", full, pss);

    return;
}

// DELETE: remove on every SS holding part of the subtree, update trie/cache/backups
typedef struct
{
    unsigned long long mask;
} Mask;

static void cb_mask(const char* p, int ss, int d, void* u)
{
    (void)p;
    (void)d;
    ((Mask *)u)->mask |= 1ULL << ss;

    return;
}

static void h_delete(Conn* c, const char* pin)
{
    char p[PATHLEN];
    int ss, isdir;
    if(np(pin, p) || !strcmp(p, "."))
    {
        reply_err(c, E_INVALID_ARGS, "invalid path");
        return;
    }

    pthread_mutex_lock(&g_mu);

    if(!lookup_locked(p, &ss, &isdir))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_FOUND, "%s does not exist", p);
        return;
    }

    if(path_busy_locked(p))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_FILE_BUSY, "%s is being written", p);
        return;
    }

    Mask m = {1ULL << ss};
    trie_walk(g_trie, p, cb_mask, &m);
    SS cp[MAX_SS];
    int down = 0;
    memcpy(cp, g_ss, sizeof cp);

    int i;
    for(i=0 ; i<g_nss ; i++)
    {
        if((m.mask >> i & 1) && !g_ss[i].alive)
            down = 1;
    }

    pthread_mutex_unlock(&g_mu);

    if(down)
    {
        reply_err(c, E_SS_DOWN, "a storage server holding %s is down (read-only)", p);
        return;
    }

    for(i=0 ; i<MAX_SS ; i++)
    {
        if(m.mask >> i & 1)
        {
            char r[MAXLINE];
            if(ss_rpc(&cp[i], r, sizeof r, "DELETE %s", p) < 0)
            {
                reply_err(c, E_SS_DOWN, "SS%d unreachable", i);
                return;
            }

            log_msg("ACK from SS%d: %s", i, r);
        }
    }
    
    pthread_mutex_lock(&g_mu);
    size_t n = trie_remove_subtree(g_trie, p);
    lru_remove_under(g_lru, p);
    pthread_mutex_unlock(&g_mu);

    for(i=0 ; i<MAX_SS ; i++)
    {
        if(m.mask >> i & 1)
            replicate_async(i, p, 0);
    }

    reply_ok(c, "deleted %s (%zu entries)", p, n);

    return;
}

// COPY <src> <dst>: NM orchestrates SS->SS transfer (spec ex.4)
static void h_copy(Conn* c, const char* sin, const char* din)
{
    char src[PATHLEN], dst[PATHLEN], target[PATHLEN], par[PATHLEN];
    if(np(sin, src) || np(din, dst) || !strcmp(src, "."))
    {
        reply_err(c, E_INVALID_ARGS, "invalid path");
        return;
    }

    int sss, sdir, ddir, t1, t2;
    int dss = -1;
    const char* base = strrchr(src, '/');

    if (base)
        base = base + 1;
    else
        base = src;

    pthread_mutex_lock(&g_mu);

    if(!lookup_locked(src, &sss, &sdir))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NOT_FOUND, "source %s does not exist", src);
        return;
    }

    if(!strcmp(dst, ".")) {
        snprintf(target, sizeof target, "%s", base);
        dss = -2;
    }
    else if(lookup_locked(dst, &dss, &ddir)) // existing dir: copy into it
    {
        if(!ddir)
        {
            pthread_mutex_unlock(&g_mu);
            reply_err(c, E_ALREADY_EXISTS, "destination %s is an existing file", dst);
            return;
        }

        path_join(dst, base, target, sizeof target);
    }
    else // new name: parent must exist
    {
        snprintf(target, sizeof target, "%s", dst);
        char* sl = strrchr(dst, '/');
        if(sl)
        {
            snprintf(par, sizeof par, "%.*s", (int)(sl - dst), dst);
            if(!lookup_locked(par, &dss, &ddir) || !ddir)
            {
                pthread_mutex_unlock(&g_mu);
                reply_err(c, E_NOT_FOUND, "destination parent %s is not a directory", par);
                return;
            }
        }
        else
            dss = -2;
    }

    if(dss == -2)
    {
        if (g_ss[sss].alive)
            dss = sss;
        else
            dss = pick_alive_locked();
    }

    if(dss < 0)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_NO_SS, "no storage server available");
        return;
    }

    if(trie_find(g_trie, target, &t1, &t2))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_ALREADY_EXISTS, "%s already exists", target);
        return;
    }

    if(path_is_under(target, src))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_INVALID_ARGS, "cannot copy %s into itself", src);
        return;
    }

    if(path_busy_locked(src) || path_busy_locked(target))
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_FILE_BUSY, "source or destination is being written");
        return;
    }

    if(!g_ss[dss].alive)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_SS_DOWN, "destination storage server is down");
        return;
    }

    char sip[64], sact[FULLLEN];
    int sport;
    if(resolve_read_locked(sss, src, sip, &sport, sact) < 0)
    {
        pthread_mutex_unlock(&g_mu);
        reply_err(c, E_SS_DOWN, "source storage server down, no backup");
        return;
    }

    SS d = g_ss[dss];

    pthread_mutex_unlock(&g_mu);

    int fd = ss_open(&d, 3);
    if(fd < 0)
    {
        reply_err(c, E_SS_DOWN, "destination SS unreachable");
        return;
    }

    set_timeout(fd, 300);
    send_line(fd, "COPYFROM %s %d %s %s 0", sip, sport, sact, target);

    char r[MAXLINE];
    int n = 0;
    if(recv_line(fd, r, sizeof r) < 0)
    {
        close(fd);
        reply_err(c, E_IO, "no reply from destination SS");
        return;
    }

    log_msg("ACK from SS%d: %s", dss, r);

    if(strncmp(r, "OK", 2))
    {
        close(fd);
        int code = E_IO;
        sscanf(r, "ERR %d", &code);
        reply_err(c, code, "copy failed: %s", r);
        return;
    }

    sscanf(r, "OK %d", &n);

    int i;
    for(i=0 ; i<n ; i++) // register every new path (dynamic update)
    {
        char e[PATHLEN + 4];
        if(recv_line(fd, e, sizeof e) < 0)
            break;
        
        pthread_mutex_lock(&g_mu);
        trie_insert(g_trie, e + 2, dss, e[0] == 'D');
        pthread_mutex_unlock(&g_mu);
    }

    close(fd);
    replicate_async(dss, target, 1);
    reply_ok(c, "copied %s -> %s (%d entries) on SS%d", src, target, n, dss);

    return;
}

// LIST (all accessible paths) and LS <dir> (children of a folder)
typedef struct
{
    Buf b;
    int cnt;
    const char *dir;
} Out;

static void cb_list(const char* p, int ss, int d, void* u)
{
    Out* o = u;
    char l[PATHLEN + 32];
    snprintf(l, sizeof l, "%s\tSS%d\t%c\n", p, ss, d ? 'D' : 'F');
    buf_add(&o->b, l);
    o->cnt++;

    return;
}

static void cb_ls(const char* p, int ss, int d, void* u)
{
    Out* o = u;
    const char* rel = p;

    if(strcmp(o->dir, "."))
    {
        size_t l = strlen(o->dir);
        if(strlen(p) <= l)
            return;
        rel = p + l + 1;
    }

    if(strchr(rel, '/'))
        return; // immediate children only
    
    char l[PATHLEN + 32];
    char type;
    if (d)
        type = 'D';
    else
        type = 'F';
    
    snprintf(l, sizeof l, "%s\t%c\tSS%d\n", rel, type, ss);
    buf_add(&o->b, l);
    o->cnt++;

    return;
}

static void h_list(Conn* c, const char* din, int ls)
{
    char d[PATHLEN];
    int ss, isdir;
    Out o = {{0}, 0, d};

    if(np(din, d)){
        reply_err(c, E_INVALID_ARGS, "invalid path");
        return;
    }

    pthread_mutex_lock(&g_mu);

    if(ls && strcmp(d, "."))
    {
        if(!lookup_locked(d, &ss, &isdir))
        {
            pthread_mutex_unlock(&g_mu);
            reply_err(c, E_NOT_FOUND, "%s does not exist", d);
            return;
        }

        if(!isdir)
        {
            pthread_mutex_unlock(&g_mu);
            reply_err(c, E_NOT_DIR, "%s is not a directory", d);
            return;
        }
    }

    const char* walk_path;
    void (*walk_cb)(const char *, int, int, void *);
    if(ls)
    {
        if(strcmp(d, "."))
            walk_path = d;
        else
            walk_path = "";
        walk_cb = cb_ls;
    }
    else
    {
        walk_path = "";
        walk_cb = cb_list;
    }

    trie_walk(g_trie, walk_path, walk_cb, &o);

    pthread_mutex_unlock(&g_mu);

    send_line(c->fd, "OK %d", o.cnt);
    if(o.b.n)
        send_all(c->fd, o.b.p, o.b.n);
    log_msg("RESP -> %s:%d OK %d entries", c->ip, c->port, o.cnt);
    buf_free(&o.b);

    return;
}

// connection dispatch
static void client_session(Conn* c, char* hello)
{
    int np_ = 0;
    sscanf(hello, "CLIENT %d", &np_);
    c->nport = np_;
    send_line(c->fd, "OK welcome");
    log_msg("CLIENT connected %s:%d (notify port %d)", c->ip, c->port, np_);
    
    char line[MAXLINE], cmd[16], a1[PATHLEN], a2[PATHLEN], a3[PATHLEN];
    while(recv_line(c->fd, line, sizeof(line)) >= 0)
    {
        if(!(*line))
            continue;
        
        log_msg("REQ <- %s:%d %s", c->ip, c->port, line);

        a1[0] = a2[0] = a3[0] = 0;
        int n = sscanf(line, "%15s %1023s %1023s %1023s", cmd, a1, a2, a3);

        send_line(c->fd, "ACK"); // initial ACK, NM never blocks others (spec 3.2)

        if(n < 1)
            continue;
        if(!strcmp(cmd, "READ") || !strcmp(cmd, "STREAM") || !strcmp(cmd, "INFO"))
        {
            if(n < 2)
                reply_err(c, E_INVALID_ARGS, "usage: %s <path>", cmd);
            else
                h_access(c, cmd, a1);
        }
        else if(!strcmp(cmd, "WRITE"))
        {
            if(n < 2)
                reply_err(c, E_INVALID_ARGS, "usage: WRITE <path>");
            else
                h_write(c, a1);
        }
        else if(!strcmp(cmd, "CREATE"))
        {
            if(n < 3)
                reply_err(c, E_INVALID_ARGS, "usage: CREATE <dir> <name> [-d]");
            else
                h_create(c, a1, a2, n > 3 && !strcmp(a3, "-d"));
        }
        else if(!strcmp(cmd, "DELETE"))
        {
            if (n < 2)
                reply_err(c, E_INVALID_ARGS, "usage: DELETE <path>");
            else
                h_delete(c, a1);
        }
        else if(!strcmp(cmd, "COPY"))
        {
            if(n < 3)
                reply_err(c, E_INVALID_ARGS, "usage: COPY <src> <dst>");
            else
                h_copy(c, a1, a2);
        }
        else if(!strcmp(cmd, "LIST"))
            h_list(c, ".", 0);
        else if(!strcmp(cmd, "LS"))
            h_list(c, n > 1 ? a1 : ".", 1);
        else if(!strcmp(cmd, "ABORT_WRITE"))
        {
            pthread_mutex_lock(&g_mu);

            Lock* l = lock_by_token_locked(a1);
            if(l)
                l->active = 0;

            pthread_mutex_unlock(&g_mu);

            reply_ok(c, "write aborted");
        }
        else
            reply_err(c, E_UNKNOWN_CMD, "unknown command %s", cmd);
    }

    log_msg("CLIENT disconnected %s:%d", c->ip, c->port);

    return;
}

static void* conn_thread(void* a)
{
    Conn* c = a;
    char line[MAXLINE];
    set_timeout(c->fd, 0);

    if(recv_line(c->fd, line, sizeof line) >= 0)
    {
        if(!strncmp(line, "CLIENT", 6))
            client_session(c, line);
        else if(!strncmp(line, "SS_REG", 6))
            handle_register(c, line);
        else if(!strncmp(line, "WRITE_DONE", 10))
            handle_write_done(c, line);
        else
            log_msg("unknown message from %s:%d: %.80s", c->ip, c->port, line);
    }

    close(c->fd);
    free(c);

    return NULL;
}

int main(int argc, char **argv)
{
    int port = 0;
    int o;
    const char* lf = "nm.log";
    while((o = getopt(argc, argv, "p:l:")) != -1)
    {
        if(o == 'p')
            port = atoi(optarg);
        else if(o == 'l')
            lf = optarg;
        else
            return 1;
    }

    if(!port)
    {
        fprintf(stderr, "usage: %s -p <port> [-l logfile]\n", argv[0]);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    log_init("NM", lf);
    g_trie = trie_new();
    g_lru  = lru_new(64);

    int ls = net_listen(port);
    if(ls < 0)
    {
        perror("listen");
        return 1;
    }

    char ip[64];
    get_local_ip(ip, sizeof ip);
    log_msg("Naming Server listening on %s:%d", ip, port);

    pthread_t t;
    pthread_create(&t, NULL, hb_thread, NULL);
    pthread_detach(t);

    for(;;) // accept loop: one thread per connection
    {
        int fd = accept(ls, NULL, NULL);
        if(fd < 0)
            continue;
        Conn* c = calloc(1, sizeof(*c));
        c->fd = fd;
        peer_str(fd, c->ip, sizeof c->ip, &c->port);

        pthread_create(&t, NULL, conn_thread, c);
        pthread_detach(t);
    }

    return 0;
}
