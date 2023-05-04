#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <math.h>
#include <limits.h>

#define KSIZE (16)
#define VSIZE (1000)

#define LINE "+-----------------------------+----------------+------------------------------+-------------------+\n"
#define LINE1 "---------------------------------------------------------------------------------------------------\n"
#define LINEt "---------------------------------------------------------------------------------------------------"

long long get_ustime_sec(void);
void _random_key(char *key,int length);

void *_write_test(void *arg);
void *_read_test(void *arg);

struct data_s
{
	long int count;
	int r;
	int thread;
	int start;
	int stop;
	int wichThread;
};
typedef struct data_s data_t;


//Statistics of all threads
long int total_write_count;	//its for the print.Total counts.
long int total_read_count;	//its for the print.Total counts.
double total_write_cost;	//running time for all threads doing write
double total_read_cost;		//running time for all threads doing read
int found;			//how many keys did the read method found

long long first_read_start_time;
long long last_read_stop_time;
long long first_write_start_time;
long long last_write_stop_time;

//-------------------------------
pthread_mutex_t m_total_write_cost;
pthread_mutex_t m_total_read_cost;

pthread_mutex_t m_total_write_count;
pthread_mutex_t m_total_read_count;

pthread_mutex_t m_found;
//-------------------------------


//------------------------
#include "../engine/db.h"
#define DATAS ("testdb")
DB* db;
//------------------------