// O(1) get/put LRU cache
#include "lru.h"
#include "common.h"
#define NB 256

typedef struct LNode
{
    char key[PATHLEN];
    int ss, isdir;
    struct LNode* prev;
    struct LNode* next;
    struct LNode* hnext;
} LNode;

struct LRU
{
    int cap;
    int size;
    long hits;
    long misses;
    LNode* head;
    LNode* tail;
    LNode* b[NB];
};

static unsigned hsh(const char* s)
{
    unsigned h = 5381;
    while(*s)
        h = h * 33 + (unsigned char)(*(s++));
    return h % NB;
}

LRU* lru_new(int cap)
{
    LRU* c = calloc(1, sizeof(*c));
    c->cap = cap;
    return c;
}

// list helpers
static void l_unlink(LRU* c, LNode* n)
{
    if(n->prev)
        n->prev->next = n->next;
    else
        c->head = n->next;

    if(n->next)
        n->next->prev = n->prev;
    else
        c->tail = n->prev;

    return;
}

static void l_front(LRU* c, LNode* n)
{
    n->prev = NULL;
    n->next = c->head;
    if(c->head)
        c->head->prev = n;
    c->head = n;
    if(!c->tail)
        c->tail = n;

    return;
}

static LNode* h_find(LRU* c, const char* k)
{
    LNode* n;
    for (n = c->b[hsh(k)] ; n ; n = n->hnext)
    {
        if (!strcmp(n->key, k))
            return n;
    }

    return NULL;
}

static void drop(LRU* c, LNode* n)
{
    LNode** pp = &c->b[hsh(n->key)];
    while (*pp != n)
        pp = &(*pp)->hnext;
    *pp = n->hnext;

    l_unlink(c, n);
    free(n);
    c->size--;

    return;
}

// lookup + promote to most-recent
int lru_get(LRU* c, const char* k, int* ss, int* isdir)
{
    LNode* n = h_find(c, k);
    if(!n)
    {
        c->misses++;
        return 0;
    }

    c->hits++;
    l_unlink(c, n);
    l_front(c, n);
    *ss = n->ss;
    *isdir = n->isdir;

    return 1;
}

// insert/update; evict least-recent when full
void lru_put(LRU* c, const char* k, int ss, int isdir)
{
    LNode* n = h_find(c, k);
    if(n)
    {
        n->ss = ss;
        n->isdir = isdir;
        l_unlink(c, n);
        l_front(c, n);
        return;
    }

    if (c->size >= c->cap && c->tail)
        drop(c, c->tail);

    n = calloc(1, sizeof(*n));
    snprintf(n->key, sizeof n->key, "%s", k);
    n->ss = ss;
    n->isdir = isdir;

    unsigned h = hsh(k);
    n->hnext = c->b[h];
    c->b[h] = n;
    l_front(c, n);
    c->size++;

    return;
}

void lru_remove(LRU* c, const char* k)
{
    LNode* n = h_find(c, k);
    if(n)
        drop(c, n);

    return;
}

void lru_remove_under(LRU* c, const char* p)
{
    LNode* n;
    LNode* nx;
    for (n = c->head ; n ; n = nx)
    {
        nx = n->next;
        if (path_is_under(n->key, p))
            drop(c, n);
    }

    return;
}

void lru_stats(LRU* c, long* h, long* m)
{
    *h = c->hits;
    *m = c->misses;

    return;
}
