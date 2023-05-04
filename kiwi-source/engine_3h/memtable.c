#include <string.h>
#include <assert.h>
#include "memtable.h"
#include "db.h"
#include "utils.h"
#include "indexer.h"

#include <stdio.h>
#include <sys/time.h>


MemTable* memtable_new(Log* log)
{
    MemTable* self = malloc(sizeof(MemTable));

    if (!self)
        PANIC("NULL allocation");

    self->list = skiplist_new(SKIPLIST_SIZE);
    skiplist_acquire(self->list);

    self->needs_compaction = 0;
    self->add_count = 0;
    self->del_count = 0;

    self->log = log;
    self->lsn = 0;

    log_recovery(log, self->list);

    //-----------------------------------
    pthread_mutex_init(&m_readers, NULL);
    pthread_cond_init(&can_read, NULL);
    pthread_cond_init(&can_write, NULL);
    //-----------------------------------

    return self;
}

void memtable_reset(MemTable* self)
{
    if (self->list)
        skiplist_release(self->list);

    self->list = skiplist_new(SKIPLIST_SIZE);
    skiplist_acquire(self->list);

    log_next(self->log, ++self->lsn);

    self->needs_compaction = 0;
    self->add_count = 0;
    self->del_count = 0;
}

void memtable_free(MemTable* self)
{
    if (self->list)
        skiplist_release(self->list);
    free(self);
}

static int _memtable_edit(MemTable* self, const Variant* key, const Variant* value, OPT opt,    int i, char* writting_key, pthread_t t_id, int wichThread, int thread)
{
    // Here we need to insert the new node that has as a skipnode's key
    // an encoded string that encompasses both the key and value supplied
    // by the user.
    //

    size_t klen = varint_length(key->length);          // key length
    size_t vlen = (opt == DEL) ? 1 : varint_length(value->length + 1);    // value length - 0 is reserved for tombstone
    size_t encoded_len = klen + vlen + key->length + value->length;

    if (opt == DEL)
        assert(value->length == 0);

    char *mem = malloc(encoded_len);
    char *node_key = mem;

    encode_varint32(node_key, key->length);
    node_key += klen;

    memcpy(node_key, key->mem, key->length);
    node_key += key->length;

    if (opt == DEL)
        encode_varint32(node_key, 0);
    else
        encode_varint32(node_key, value->length + 1);

    node_key += vlen;
    memcpy(node_key, value->mem, value->length);

    self->needs_compaction = log_append(self->log, mem, encoded_len);

     //==============================
    pthread_mutex_lock(&m_writers);			
	
    //printf("WRITER readers: %d   writers: %d\n", readers, writers);

    //sleep(1);
    waiting_writers += 1;
    while( writers > 0){			
	//printf("Writer thread %ld is waiting\n",pthread_self());
	//pthread_mutex_lock(&m_readers);

	pthread_cond_wait(&can_write, &m_writers);	
	//pthread_mutex_lock(&m_writers);
	//pthread_cond_wait(&can_read, &m_readers);
    }
    
    if(thread == 1)
    	fprintf(stderr, "%d adding %s, by thread: %ld ----- started %dst\n", i, writting_key, t_id, wichThread);
    writers += 1;
    
    //pthread_cond_wait(&can_read , &m_readers);
    
    pthread_mutex_unlock(&m_writers);			
    //==============================

    //sleep(1);
	
    

    if (skiplist_insert(self->list, key->mem, key->length, opt, mem) == STATUS_OK_DEALLOC)
        free(mem);

    if (opt == ADD)
        self->add_count++;
    else
        self->del_count++;

//    DEBUG("memtable_edit: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, opt);


    //==============================
    pthread_mutex_lock(&m_writers);			
    writers -= 1;					
    waiting_writers -= 1;
    if(thread == 1)    
	printf("waiting_writers: %d, writer:%d\n", waiting_writers, writers);

    

    pthread_cond_signal(&can_write);		
    //pthread_mutex_unlock(&m_writers);   		

    pthread_cond_broadcast(&can_read);
    pthread_mutex_unlock(&m_readers); 

    pthread_mutex_unlock(&m_writers);
    //==============================

    return 1;
}

int memtable_add(MemTable* self, const Variant* key, const Variant* value,    int i, char* writting_key, pthread_t t_id, int wichThread, int thread)
{
    return _memtable_edit(self, key, value, ADD,     i,  writting_key,  t_id,  wichThread, thread);
}

int memtable_remove(MemTable* self, const Variant* key,    int i, char* writting_key, pthread_t t_id, int wichThread, int thread)
{
    Variant value;
    value.length = 0;
    return _memtable_edit(self, key, &value, DEL,    i,  writting_key,  t_id,  wichThread, thread);
}

int memtable_get(SkipList* list, const Variant *key, Variant* value,    int i, char* reading_key, pthread_t t_id, int wichThread, int thread)
{
    //=====================================
    pthread_mutex_lock(&m_readers);			

    if(thread == 1)
	printf("READER readers: %d   writers: %d\n", readers, writers);
    //sleep(4);

    //if(waiting_writers > 0){
	//pthread_cond_wait(&can_read, &m_readers);
    while(writers > 0){ //false				
	printf("Reader thread %ld is waiting\n",pthread_self());
	pthread_cond_wait(&can_read , &m_readers);	
    }
    //sleep(4);

    if(thread == 1)
	fprintf(stderr, "%d searching %s, by thread: %ld -----started %dst\n", i, reading_key, t_id, wichThread);
    readers += 1;
    
    //pthread_mutex_unlock(&m_writers);

    //pthread_cond_signal(&can_write);
    pthread_mutex_unlock(&m_readers);			
    //=====================================	

    SkipNode* node = skiplist_lookup(list, key->mem, key->length);

    if (!node){
     	
	//=====================================
	pthread_mutex_lock(&m_readers);			
	readers -= 1;					
	//printf("readers: %d\n", readers);
	    

	pthread_cond_broadcast(&can_read);   		
	//pthread_mutex_unlock(&m_readers);			

	pthread_cond_signal(&can_write);
	pthread_mutex_unlock(&m_writers);
	    
	pthread_mutex_unlock(&m_readers);
	//=====================================

	return 0;
    }

    const char* encoded = node->data;
    encoded += varint_length(key->length) + key->length;

    uint32_t encoded_len = 0;
    encoded = get_varint32(encoded, encoded + 5, &encoded_len);

    if (encoded_len > 1)
        buffer_putnstr(value, encoded, encoded_len - 1);
    else{
     	
	//=====================================
	pthread_mutex_lock(&m_readers);			
	readers -= 1;					
	//printf("readers: %d\n", readers);
	    

	pthread_cond_broadcast(&can_read);   		
	//pthread_mutex_unlock(&m_readers);			

	pthread_cond_signal(&can_write);
	pthread_mutex_unlock(&m_writers);
	    
	pthread_mutex_unlock(&m_readers);
	//=====================================

	return 0;
    }

        //=====================================
	pthread_mutex_lock(&m_readers);			
	readers -= 1;					
	//printf("readers: %d\n", readers);
	    

	pthread_cond_broadcast(&can_read);   		
	//pthread_mutex_unlock(&m_readers);			

	pthread_cond_signal(&can_write);
	pthread_mutex_unlock(&m_writers);
	    
	pthread_mutex_unlock(&m_readers);
	//=====================================

    	return 1;
}

int memtable_needs_compaction(MemTable *self)
{
    return (self->needs_compaction ||
            self->list->count >= SKIPLIST_SIZE ||
            self->list->allocated >= MAX_SKIPLIST_ALLOCATION);
}

void memtable_extract_node(SkipNode* node, Variant* key, Variant* value, OPT* opt)
{
    uint32_t length = 0;
    const char* encoded = node->data;
    encoded = get_varint32(encoded, encoded + 5, &length);

    buffer_clear(key);
    buffer_putnstr(key, encoded, length);

    encoded += length;

    length = 0;
    encoded = get_varint32(encoded, encoded + 5, &length);

    if (length == 0)
        *opt = DEL;
    else
        *opt = ADD;

    if (value)
    {
        buffer_clear(value);

        if (*opt == ADD)
            buffer_putnstr(value, encoded, length - 1);

//        DEBUG("memtable_extract_node: %.*s %.*s opt: %d", key->length, key->mem, value->length, value->mem, *opt);
    }
}
