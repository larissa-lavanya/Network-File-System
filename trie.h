// prefix tree mapping global path -> owning storage server (fast NM lookup)
#ifndef TRIE_H
#define TRIE_H
typedef struct Trie Trie;
typedef void (*trie_cb)(const char *path, int ss, int isdir, void *ud);
Trie *trie_new(void);
int trie_insert(Trie *t, const char *path, int ss, int isdir);     // 0 ok, -1 already present
int trie_find(Trie *t, const char *path, int *ss, int *isdir);     // 1 found
size_t trie_remove_subtree(Trie *t, const char *path);             // removes path + descendants
void trie_walk(Trie *t, const char *prefix, trie_cb cb, void *ud); // "" = all; else prefix and its descendants
size_t trie_count(Trie *t);
#endif
