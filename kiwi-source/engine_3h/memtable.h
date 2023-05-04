#ifndef __MEMTABLE_H__
#define __MEMTABLE_H__

#include "skiplist.h"
#include "variant.h"
#include "log.h"

typedef struct _memtable {
    SkipList* list;

    int lsn;
    Log* log;

    uint32_t needs_compaction;
    uint32_t del_count;
    uint32_t add_count;
} MemTable;

MemTable* memtable_new(Log* log);
void memtable_reset(MemTable* self);
void memtable_free(MemTable* self);

int memtable_add(MemTable* self, const Variant *key, const Variant *value,    int i, char* writting_key, pthread_t t_id, int wichThread, int thread);
int memtable_remove(MemTable* self, const Variant* key,    int i, char* reading_key, pthread_t t_id, int wichThread, int thread);
int memtable_get(SkipList* list, const Variant *key, Variant* value,    int i, char* reading_key, pthread_t t_id, int wichThread, int thread);


// Utility function
int memtable_needs_compaction(MemTable* self);
void memtable_extract_node(SkipNode* node, Variant* key, Variant* value, OPT* opt);


//-----------------------------------

pthread_mutex_t m_readers;

pthread_cond_t can_read;
pthread_cond_t can_write;
int readers;
int writers;
int waiting_writers;
//-----------------------------------


#endif
