// character trie (first-child/next-sibling nodes keep memory small)
#include <stdlib.h>
#include <string.h>
#include "trie.h"
#define TCAP 2200

typedef struct TNode
{
    char c;
    struct TNode* child;
    struct TNode* sib;
    int term, ss, isdir;
} TNode;

struct Trie
{
    TNode root;
    size_t count;
};

Trie* trie_new(void)
{
    return calloc(1, sizeof(Trie));
}

size_t trie_count(Trie* t)
{
    return t->count;
}

// find child by character
static TNode* child_of(TNode* n, char c)
{
    TNode* x;
    for (x=n->child ; x ; x=x->sib)
    {
        if(x->c == c)
            return x;
    }

    return NULL;
}

static TNode* get_or_make(TNode* n, char c)
{
    TNode* x = child_of(n, c);
    if(x)
        return x;

    x = calloc(1, sizeof *x);
    x->c = c;
    x->sib = n->child;
    n->child = x;

    return x;
}

static TNode* walk_to(Trie* t, const char* k)
{
    TNode* n = &t->root;
    for( ; n && *k ; k++)
        n = child_of(n, *k);
    
    return n;
}

// insert a key; refuses duplicates
int trie_insert(Trie* t, const char* k, int ss, int isdir)
{
    TNode* n = &t->root;
    for( ; *k ; k++)
        n = get_or_make(n, *k);

    if(n->term)
        return -1;

    n->term  = 1;
    n->ss    = ss;
    n->isdir = isdir;
    t->count++;

    return 0;
}

// exact lookup
int trie_find(Trie* t, const char* k, int* ss, int* isdir)
{
    TNode* n = walk_to(t, k);
    if(!n || !n->term)
        return 0;

    *ss    = n->ss;
    *isdir = n->isdir;

    return 1;
}

// free everything below n, return number of terminal keys freed
static size_t free_all(TNode* n)
{
    size_t cnt = 0;
    TNode* x;
    for(x=n->child ; x ; )
    {
        TNode* nx = x->sib;
        if(x->term)
            cnt += 1;
        cnt += free_all(x);
        free(x);
        x = nx;
    }
    n->child = NULL;

    return cnt;
}

static void unlink_child(TNode* n, TNode* c)
{
    TNode** pp;
    for(pp=&n->child ; *pp ; pp=&(*pp)->sib)
    {
        if(*pp == c)
        {
            *pp = c->sib;
            return;
        }
    }

    return;
}

// recursive removal; returns 1 when n became empty (so the caller can prune it)
static int rm_rec(TNode* n, const char* k, size_t* removed)
{
    if(!(*k))
    {
        if(n->term)
        {
            n->term = 0;
            (*removed)++;
        }

        TNode* s = child_of(n, '/'); // descendants live under "path/"
        if(s)
        {
            unlink_child(n, s);
            if(s->term)
                *removed += 1;
            *removed += free_all(s);
            free(s);
        }

        return !n->term && !n->child;
    }

    TNode* c = child_of(n, *k);
    if(!c)
        return 0;
    
    if(rm_rec(c, k + 1, removed))
    {
        unlink_child(n, c);
        free(c);
    }

    return !n->term && !n->child;
}

size_t trie_remove_subtree(Trie* t, const char* k)
{
    size_t removed = 0;
    rm_rec(&t->root, k, &removed);
    t->count -= removed;
    return removed;
}

// depth-first visit of all terminal keys below n
static void dfs(TNode* n, char* buf, size_t len, trie_cb cb, void* ud)
{
    if(n->term)
    {
        buf[len] = 0;
        cb(buf, n->ss, n->isdir, ud);
    }

    TNode* x;
    for (x=n->child ; x ; x=x->sib)
    {
        if(len + 1 < TCAP)
        {
            buf[len] = x->c;
            dfs(x, buf, len + 1, cb, ud);
        }
    }

    return;
}

void trie_walk(Trie* t, const char* prefix, trie_cb cb, void* ud)
{
    char buf[TCAP];
    if(!(*prefix))
    {
        dfs(&t->root, buf, 0, cb, ud);
        return;
    }

    TNode* n = walk_to(t, prefix);
    if(!n)
        return;
    
    size_t l = strlen(prefix);
    memcpy(buf, prefix, l);

    if(n->term)
    {
        buf[l] = 0;
        cb(buf, n->ss, n->isdir, ud);
    }

    TNode* s = child_of(n, '/');
    if(s)
    {
        buf[l] = '/';
        dfs(s, buf, l + 1, cb, ud);
    }

    return;
}
