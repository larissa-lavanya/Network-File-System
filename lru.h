// lru.h - fixed-capacity LRU cache (hash map + doubly linked list) for recent path lookups
#ifndef LRU_H
#define LRU_H
typedef struct LRU LRU;
LRU *lru_new(int cap);
int lru_get(LRU *c, const char *key, int *ss, int *isdir); // 1 on hit (promotes entry)
void lru_put(LRU *c, const char *key, int ss, int isdir);
void lru_remove(LRU *c, const char *key);
void lru_remove_under(LRU *c, const char *path); // drop path and everything below it
void lru_stats(LRU *c, long *hits, long *misses);
#endif
