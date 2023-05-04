#include <string.h>
//#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"

void *_write_test(void *arg)
{
	data_t *d = (data_t *) arg;

	long int count = d->count;
	int r = d->r;
	int thread = d->thread;
	
	pthread_mutex_lock(&m_total_write_count);
	total_write_count += d->stop - d->start+1;
	pthread_mutex_unlock(&m_total_write_count);
		
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);
	
	printf("Write     start: %d,       stop: %d\n", d->start, d->stop);

	start = get_ustime_sec();

	pthread_mutex_lock(&m_total_write_cost);
	if(start < first_write_start_time)
		first_write_start_time = start;
	pthread_mutex_unlock(&m_total_write_cost);

	for (i = d->start; i <= d->stop; i++) {
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);

		if(thread != 1) // no threads
			fprintf(stderr, "%d adding %s\n", i, key);

		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv,   i, key, pthread_self(), d->wichThread, d->thread);
		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	if(thread != 1)
		db_close(db);

	end = get_ustime_sec();
	cost = end -start;

	pthread_mutex_lock(&m_total_write_cost);
	//total_write_cost += cost;
	if(end > last_write_stop_time)
		last_write_stop_time = end;
	pthread_mutex_unlock(&m_total_write_cost);

	if(thread == 1){
		printf(LINEt);
		printf("%ld\n", pthread_self());
	}else
		printf(LINE);
	if(thread != 1)
		printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
			,count, (double)(cost / count)
			,(double)(count / cost)
			,cost);	

	return 0;
}

void *_read_test(void *arg)
{
	data_t *d = (data_t *) arg;

	long int count = d->count;
	int r = d->r;
	int thread = d->thread;

	pthread_mutex_lock(&m_total_read_count);
	total_read_count += d->stop - d->start + 1;
	pthread_mutex_unlock(&m_total_read_count);
	
	int i;
	int ret;
	double cost = 0;
	long long start,end;
	Variant sk;
	Variant sv;
	char key[KSIZE + 1];

	start = get_ustime_sec();

	pthread_mutex_lock(&m_total_read_cost);
	if(start < first_read_start_time)
		first_read_start_time = start;
	pthread_mutex_unlock(&m_total_read_cost);

	printf("read     start: %d,       stop: %d\n", d->start, d->stop);
	
	for (i = d->start; i <= d->stop; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		
		if(thread != 1) // no threads
			fprintf(stderr, "%d searching %s\n", i, key);

		sk.length = KSIZE;
		sk.mem = key;
		
		ret = db_get(db, &sk, &sv,   i,key,pthread_self(), d->wichThread, d->thread);
		
		if (ret) {
			//db_free_data(sv.mem);
			pthread_mutex_lock(&m_found);
			found++;
			//printf("found %d\n",found);
			pthread_mutex_unlock(&m_found);
			
		} else {
			INFO("not found key#%s", 
					sk.mem);
  		}
	
		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}

	if(thread != 1)
		db_close(db);

	end = get_ustime_sec();
	cost = end - start;

	pthread_mutex_lock(&m_total_read_cost);
	if(end > last_read_stop_time)
		last_read_stop_time = end;
	pthread_mutex_unlock(&m_total_read_cost);

	if(thread == 1){
		printf(LINEt);
		printf("%ld\n", pthread_self());
	}else
		printf(LINE);
	if(thread != 1)
		printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
			count, found,
			(double)(cost / count),
			(double)(count / cost),
			cost);

	return 0;
}