// Usage: ./client -n <nm_ip> -p <nm_port> [-e "CMD"]... [-w secs]
#include "common.h"

#define ACK_TIMEOUT 5   // seconds to wait for NM's initial ACK (spec 3.2)
#define RESP_TIMEOUT 300    // seconds to wait for final response

static int g_nm = -1;

// async notification listener: NM pushes completion/failure of async writes (spec 3.1)
static void* notify_thread(void* a)
{
    int ls = (long int)a;
    char line[MAXLINE];
    for(;;)
    {
        int fd = accept(ls, NULL, NULL);
        if(fd < 0)
            continue;
        
        set_timeout(fd, 5);

        if(recv_line(fd, line, sizeof line) > 0 && !strncmp(line, "NOTIFY ", 7))
        {
            printf("\n[NOTIFY] %s\n", line + 7);
            fflush(stdout);
        }
        close(fd);
    }

    return NULL;
}

// request to NM: wait ACK then final reply
static int nm_request(const char* line, char* resp, size_t n)
{
    if(send_line(g_nm, "%s", line))
    {
        printf("[ERROR] connection to Naming Server lost\n");
        return -1;
    }

    char ack[MAXLINE];
    set_timeout(g_nm, ACK_TIMEOUT);
    if(recv_line(g_nm, ack, sizeof ack) < 0)
    {
        printf("[ERROR %d TIMEOUT] no ACK from Naming Server within %ds\n", E_TIMEOUT, ACK_TIMEOUT);
        return -1;
    }

    set_timeout(g_nm, RESP_TIMEOUT);
    if(strcmp(ack, "ACK"))
        strncpy(resp, ack, n); // tolerate servers that skip the ACK
    else if(recv_line(g_nm, resp, n) < 0)
    {
        printf("[ERROR %d TIMEOUT] no final response from Naming Server\n", E_TIMEOUT);
        return -1;
    }

    if(!strncmp(resp, "ERR", 3)) // error code display (spec 3.3)
    { 
        int code  = 0;
        char *msg = resp + 3;
        sscanf(resp, "ERR %d", &code);
        while(*msg == ' ')
            msg++;
        while(isdigit((unsigned char)*msg))
            msg++;
        while(*msg == ' ')
            msg++;
        printf("[ERROR %d %s] %s\n", code, err_name(code), msg);
        return -1;
    }
    return 0;
}

// parse "OK ip port path token"
static int parse_loc(const char* r, char* ip, int* port, char* path, char* tok)
{
    if (sscanf(r, "OK %63s %d %1023s %31s", ip, port, path, tok) == 4)
        return 0;
    return -1;
}

// direct SS interactions
// receive DATA packets until STOP and hand each to sink (stdout or music player)
static int pull_data(const char* ip, int port, const char* path, FILE* sink)
{
    int fd = net_connect(ip, port, 5);
    if(fd < 0)
    {
        printf("[ERROR %d SS_DOWN] cannot reach storage server %s:%d\n", E_SS_DOWN, ip, port);
        return -1;
    }

    send_line(fd, "READ %s", path);
    char line[MAXLINE], buf[CHUNK];
    int rc = 0;
    for(;;)
    {
        if(recv_line(fd, line, sizeof line) < 0)
        {
            printf("\n[ERROR %d IO_ERROR] connection to storage server lost\n", E_IO);
            rc = -1;
            break;
        }
        if(!strcmp(line, "STOP"))
            break; // STOP packet ends the transfer
        if(!strncmp(line, "ERR", 3))
        {
            printf("[ERROR] %s\n", line);
            rc = -1;
            break;
        }

        size_t n = 0;
        if(sscanf(line, "DATA %zu", &n) != 1 || n > CHUNK || recv_all(fd, buf, n))
        {
            rc = -1;
            break;
        }

        fwrite(buf, 1, n, sink);
    }

    close(fd);
    return rc;
}

static void cmd_read(const char* path)
{
    char r[MAXLINE], ip[64], p[PATHLEN], tok[32];
    int port;
    if(nm_request(path, r, sizeof r) || parse_loc(r, ip, &port, p, tok))
        return;

    if(pull_data(ip, port, p, stdout) == 0)
        printf("\n[done]\n");
    
    fflush(stdout);
    return;
}

static void cmd_stream(const char* path)
{
    char r[MAXLINE], ip[64], p[PATHLEN], tok[32];
    int port;
    if(nm_request(path, r, sizeof r) || parse_loc(r, ip, &port, p, tok))
        return;
    
    const char* pl = getenv("NFS_PLAYER"); // default player: mpv reading stdin

    FILE* pipe_temp;
    if (pl != NULL)
        pipe_temp = popen(pl, "w");
    else
        pipe_temp = popen("mpv --no-video --really-quiet -", "w");
    
    if(!pipe_temp)
    {
        printf("[ERROR] cannot start music player\n");
        return;
    }

    pull_data(ip, port, p, pipe_temp);
    pclose(pipe_temp);
    printf("[stream finished]\n");

    return;
}

static void cmd_info(const char* path)
{
    char r[MAXLINE], ip[64], p[PATHLEN], tok[32], l[MAXLINE];
    int port;
    if(nm_request(path, r, sizeof r) || parse_loc(r, ip, &port, p, tok))
        return;
    
    int fd = net_connect(ip, port, 5);
    if(fd < 0)
    {
        printf("[ERROR %d SS_DOWN] cannot reach storage server\n", E_SS_DOWN);
        return;
    }

    send_line(fd, "INFO %s", p);
    if(recv_line(fd, l, sizeof l) > 0)
        printf("%s\n", l);
    else
        printf("[ERROR] no reply\n");
    
    close(fd);
    return;
}

// WRITE <path> [--SYNC] [--file F | --text TEXT...]; otherwise content is read from stdin until a line "EOF"
static void cmd_write(char* line)
{
    char* toks = strdup(line);
    char *sp, *t;
    char path[PATHLEN] = "";
    char file[PATHLEN] = "";
    int sync = 0;
    char* text = NULL;
    t = strtok_r(toks, " \t", &sp); // skip "WRITE"

    while((t = strtok_r(NULL, " \t", &sp)))
    {
        if(!strcmp(t, "--SYNC"))
            sync = 1;
        else if(!strcmp(t, "--file"))
        {
            t = strtok_r(NULL, " \t", &sp);
            if(t)
                snprintf(file, sizeof file, "%s", t);
        }
        else if(!strcmp(t, "--text"))
        {
            char* p = strstr(line, "--text");
            text = p + 6;

            while(*text == ' ')
                text++;
            
            break;
        }
        else if(!path[0])
            snprintf(path, sizeof path, "%s", t);
    }

    if(!path[0])
    {
        printf("usage: WRITE <path> [--SYNC] [--file <local>|--text <content>]\n");
        free(toks);
        return;
    }

    Buf b = {0};
    char* data = NULL;
    size_t len = 0;
    if(file[0])
    {
        FILE* f = fopen(file, "rb");
        if(!f)
        {
            printf("[ERROR] cannot open local file %s\n", file);
            free(toks);
            return;
        }

        fseek(f, 0, SEEK_END);
        len = ftell(f);
        fseek(f, 0, SEEK_SET);
        data = malloc(len + 1);

        if (fread(data, 1, len, f) != len)
            len = 0;

        fclose(f);
    }
    else if(text)
    {
        data = strdup(text);
        len  = strlen(data);
    }
    else
    {
        printf("Enter content, finish with a line containing only EOF:\n");

        char l[MAXLINE];
        while (fgets(l, sizeof l, stdin) && strcmp(l, "EOF\n") && strcmp(l, "EOF"))
            buf_add(&b, l);

        if (b.p != NULL)
            data = b.p;
        else
            data = strdup("");
        len = b.n;
    }

    char req[PATHLEN + 32], r[MAXLINE], ip[64], p[PATHLEN], tok[32];
    int port;
    snprintf(req, sizeof req, "WRITE %s", path);

    if(nm_request(req, r, sizeof r) || parse_loc(r, ip, &port, p, tok))
    {
        free(data);
        free(toks);
        return;
    }

    int fd = net_connect(ip, port, 5);
    if(fd < 0)
    {
        printf("[ERROR %d SS_DOWN] cannot reach storage server\n", E_SS_DOWN);
        snprintf(req, sizeof req, "ABORT_WRITE %s", tok);
        nm_request(req, r, sizeof r); // release NM lock
        free(data);
        free(toks);
        return;
    }

    send_line(fd, "WRITE %s %d %s %zu", p, sync, tok, len); // priority writes via --SYNC (spec 3.1)
    send_all(fd, data, len);

    char reply[MAXLINE];
    if(recv_line(fd, reply, sizeof reply) < 0)
        printf("[ERROR] no reply from storage server\n");
    else if(!strncmp(reply, "OK", 2))
        printf("[write complete] %s\n", reply + 3);
    else if(!strncmp(reply, "ACK_ASYNC", 9))
        printf("[write accepted asynchronously] %s - you will be notified when it is flushed\n", reply + 10);
    else
        printf("[ERROR] %s\n", reply);
    
    close(fd);
    free(data);
    free(toks);
    return;
}

// commands answered fully by NM
static void cmd_simple(const char* line)
{
    char r[MAXLINE];
    if (nm_request(line, r, sizeof r) == 0)
        printf("%s\n", r + 3);

    return;
}

static void cmd_listing(const char* line) // LIST / LS: header with count, then n lines
{
    char r[MAXLINE];
    int n = 0;
    if (nm_request(line, r, sizeof r))
        return;
    
    sscanf(r, "OK %d", &n);

    int i;
    for (i=0 ; i<n ; i++)
    {
        if(recv_line(g_nm, r, sizeof r) < 0)
            break;
        printf("  %s\n", r);
    }

    printf("(%d entries)\n", n);

    return;
}

static void help(void)
{
    printf("Commands:\n  READ <path>\n  WRITE <path> [--SYNC] [--file <local> | --text <content>]\n  INFO <path>\n  "
           "STREAM <path>\n"
           "  CREATE <dir|.> <name> [-d]\n  DELETE <path>\n  COPY <src> <dst>\n  LIST\n  LS [dir]\n  HELP\n  EXIT\n");

    return;
}

// dispatch one command line; returns 1 to exit
static int run(char* line)
{
    char cmd[32] = "";
    sscanf(line, "%31s", cmd);

    char* c;
    for (c = cmd ; *c ; c++)
        *c = toupper((unsigned char)*c);

    if(!*cmd)
        return 0;
    
    char* rest = strchr(line, ' ');
    if(rest != NULL)
        rest = rest + 1;
    else
        rest = "";
    
    while(*rest == ' ')
        rest++;
    
    char req[MAXLINE];
    snprintf(req, sizeof req, "%s %s", cmd, rest);

    if(!strcmp(cmd, "EXIT") || !strcmp(cmd, "QUIT"))
        return 1;
    else if(!strcmp(cmd, "HELP"))
        help();
    else if(!strcmp(cmd, "READ"))
        cmd_read(req);
    else if(!strcmp(cmd, "STREAM"))
        cmd_stream(req);
    else if(!strcmp(cmd, "INFO"))
        cmd_info(req);
    else if(!strcmp(cmd, "WRITE"))
        cmd_write(req);
    else if(!strcmp(cmd, "LIST") || !strcmp(cmd, "LS"))
        cmd_listing(req);
    else if(!strcmp(cmd, "CREATE") || !strcmp(cmd, "DELETE") || !strcmp(cmd, "COPY"))
        cmd_simple(req);
    else
        printf("unknown command (type HELP)\n");
    
    return 0;
}

int main(int argc, char **argv)
{
    char* nm_ip = NULL;
    char* cmds[64];
    int nm_port = 0, ncmd = 0, wait_s = 0;
    int o;
    while((o = getopt(argc, argv, "n:p:e:w:")) != -1)
    {
        if(o == 'n')
            nm_ip = optarg;
        else if(o == 'p')
            nm_port = atoi(optarg);
        else if(o == 'e' && ncmd < 64)
            cmds[ncmd++] = optarg;
        else if(o == 'w')
            wait_s = atoi(optarg);
        else
            return 1;
    }

    if(!nm_ip || !nm_port)
    {
        fprintf(stderr, "usage: %s -n <nm_ip> -p <nm_port> [-e \"CMD\"]... [-w secs]\n", argv[0]);
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);
    int nl = net_listen(0); // ephemeral port for NM notifications

    pthread_t t;
    pthread_create(&t, NULL, notify_thread, (void*)(long)nl);
    pthread_detach(t);

    g_nm = net_connect(nm_ip, nm_port, 5);
    if(g_nm < 0)
    {
        fprintf(stderr, "cannot connect to Naming Server %s:%d\n", nm_ip, nm_port);
        return 1;
    }

    send_line(g_nm, "CLIENT %d", net_port(nl));

    char hello[256];
    set_timeout(g_nm, 5);
    if(recv_line(g_nm, hello, sizeof hello) < 0)
    {
        fprintf(stderr, "no reply from Naming Server\n");
        return 1;
    }

    printf("Connected to Naming Server %s:%d. Type HELP.\n", nm_ip, nm_port);
    if(ncmd) // scripted mode
    {
        int i;
        for (i=0 ; i<ncmd ; i++)
        {
            printf("> %s\n", cmds[i]);
            char l[MAXLINE];
            snprintf(l, sizeof l, "%s", cmds[i]);
            if(run(l))
                break;
        }

        if(wait_s)
            sleep(wait_s);
        
        return 0;
    }

    char line[MAXLINE];
    for (;;) // interactive shell
    {
        printf("> ");
        fflush(stdout);

        if (!fgets(line, sizeof line, stdin))
            break;
        
        line[strcspn(line, "\r\n")] = 0;
        if (run(line))
            break;
    }

    return 0;
}
