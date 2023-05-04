#include "bench.h"

void _random_key(char *key, int length)
{
	int i;
	char salt[36] = "abcdefghijklmnopqrstuvwxyz0123456789";

	for (i = 0; i < length; i++)
		key[i] = salt[rand() % 36];
}

void _print_header(int count)
{
	double index_size = (double)((double)(KSIZE + 8 + 1) * count) / 1048576.0;
	double data_size = (double)((double)(VSIZE + 4) * count) / 1048576.0;

	printf("Keys:\t\t%d bytes each\n",
		   KSIZE);
	printf("Values: \t%d bytes each\n",
		   VSIZE);
	printf("Entries:\t%d\n",
		   count);
	printf("IndexSize:\t%.1f MB (estimated)\n",
		   index_size);
	printf("DataSize:\t%.1f MB (estimated)\n",
		   data_size);

	printf(LINE1);
}

void _print_environment()
{
	time_t now = time(NULL);

	printf("Date:\t\t%s",
		   (char *)ctime(&now));

	int num_cpus = 0;
	char cpu_type[256] = {0};
	char cache_size[256] = {0};

	FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
	if (cpuinfo)
	{
		char line[1024] = {0};
		while (fgets(line, sizeof(line), cpuinfo) != NULL)
		{
			const char *sep = strchr(line, ':');
			if (sep == NULL || strlen(sep) < 10)
				continue;

			char key[1024] = {0};
			char val[1024] = {0};
			strncpy(key, line, sep - 1 - line);
			strncpy(val, sep + 1, strlen(sep) - 1);
			if (strcmp("model name", key) == 0)
			{
				num_cpus++;
				strcpy(cpu_type, val);
			}
			else if (strcmp("cache size", key) == 0)
				strncpy(cache_size, val + 1, strlen(val) - 1);
		}

		fclose(cpuinfo);
		printf("CPU:\t\t%d * %s",
			   num_cpus,
			   cpu_type);

		printf("CPUCache:\t%s\n",
			   cache_size);
	}
}

int isInteger(double val)
{
	int truncated = (int)val;
	if (val == truncated)
		return 0;
	else
		return 1;
}

int main(int argc, char **argv)
{
	srand(time(NULL));
	if (argc < 3)
	{
		fprintf(stderr, "Usage: db-bench <write | read | readwrite> <count> <random> <threads>\n");
		exit(1);
	}

	long int count;

	if (strcmp(argv[1], "write") == 0)
	{
		if (argc == 3)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			db = db_open(DATAS);

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;

			_write_test((void *)&data);
		}
		else if (argc == 4)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			db = db_open(DATAS);

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;

			if (atoi(argv[3]) != 0)
				data.r = 1;
			_write_test((void *)&data);
		}
		else if (argc == 5)
		{
			int total_count = atoi(argv[2]);
			_print_header(total_count);
			_print_environment();

			db = db_open(DATAS);

			int r = 0;
			int thread = 0;

			if (atoi(argv[3]) != 0)
				r = 1;
			int threads = atoi(argv[4]);
			if (threads < 1)
			{
				printf("This can not be applyed. Threads input have to be >= 1\n");
				exit(1);
			}
			if (threads > total_count)
			{
				printf("This can not be applyed. Threads input must <= Count input\n");
				exit(1);
			}
			if (threads >= 1)
				thread = 1;

			db = db_open(DATAS);

			pthread_t id[threads];
			int i;
			int flag = 0;

			double poses_aithseis_to_ka8e_nhma = (double)total_count / threads;
			if (isInteger(poses_aithseis_to_ka8e_nhma) == 1)
				flag = 1;

			int count = (long int)floor(poses_aithseis_to_ka8e_nhma);

			first_write_start_time = LONG_MAX;
			total_write_cost = 0;
			total_write_count = 0;
			pthread_mutex_init(&m_total_write_cost, NULL);
			pthread_mutex_init(&m_total_write_count, NULL);

			data_t array_structs[threads];

			for (i = 0; i < threads; i++)
			{
				array_structs[i].r = r;
				array_structs[i].thread = thread;
				array_structs[i].wichThread = i;
				array_structs[i].start = (int)count * i;
				array_structs[i].stop = (int)count * i + count - 1;

				if ((i == threads - 1) && (flag == 1))
					array_structs[i].stop = total_count - 1;

				pthread_create(&id[i], NULL, _write_test, (void *)&array_structs[i]);
			}
			for (i = 0; i < threads; i++)
				pthread_join(id[i], NULL);

			db_close(db);

			total_write_cost = last_write_stop_time - first_write_start_time;

			printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n", total_write_count, (double)(total_write_cost / total_write_count), (double)(total_write_count / total_write_cost), total_write_cost);
		}
		else
		{
			fprintf(stderr, "Usage: db-bench <write | read | readwrite> <count> <random> <threads>\n");
			exit(1);
		}
	}
	else if (strcmp(argv[1], "read") == 0)
	{
		if (argc == 3)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			db = db_open(DATAS);

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;
			pthread_mutex_init(&m_found, NULL);

			_read_test((void *)&data);
		}
		else if (argc == 4)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;
			pthread_mutex_init(&m_found, NULL);

			db = db_open(DATAS);

			if (atoi(argv[3]) != 0)
				data.r = 1;
			_read_test((void *)&data);
		}
		else if (argc == 5)
		{
			int total_count = atoi(argv[2]);
			_print_header(total_count);
			_print_environment();

			int r = 0;
			int thread = 0;

			if (atoi(argv[3]) != 0)
				r = 1;
			int threads = atoi(argv[4]);
			if (threads < 1)
			{
				printf("This can not be applyed. Threads input must be >= 1\n");
				exit(1);
			}
			if (threads > total_count)
			{
				printf("This can not be applyed. Threads input must <= Count input\n");
				exit(1);
			}
			if (threads >= 1)
				thread = 1;

			db = db_open(DATAS);

			pthread_t id[threads];
			int i;
			int flag = 0; // an to count/threads den einai akeraios

			double poses_aithseis_to_ka8e_nhma = (double)total_count / threads;
			if (isInteger(poses_aithseis_to_ka8e_nhma) == 1)
				flag = 1;
			int count = (long int)floor(poses_aithseis_to_ka8e_nhma);

			total_read_cost = 0;
			total_read_count = 0;
			first_read_start_time = LONG_MAX;
			pthread_mutex_init(&m_total_read_cost, NULL);
			pthread_mutex_init(&m_total_read_count, NULL);
			pthread_mutex_init(&m_found, NULL);
			found = 0;
			data_t array_structs[threads];

			for (i = 0; i < threads; i++)
			{
				array_structs[i].r = r;
				array_structs[i].thread = thread;
				array_structs[i].wichThread = i;
				array_structs[i].start = (int)count * i;
				array_structs[i].stop = (int)count * i + count - 1;

				if ((i == threads - 1) && (flag == 1))
					array_structs[i].stop = total_count - 1;

				pthread_create(&id[i], NULL, _read_test, (void *)&array_structs[i]);
			}
			for (i = 0; i < threads; i++)
				pthread_join(id[i], NULL);

			db_close(db);
			total_read_cost = last_read_stop_time - first_read_start_time;

			printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
				   total_read_count, found,
				   (double)(total_read_cost / total_read_count),
				   (double)(total_read_count / total_read_cost),
				   total_read_cost);
		}
		else
		{
			fprintf(stderr, "Usage: db-bench <write | read | readwrite> <count> <random> <threads>\n");
			exit(1);
		}
	}
	else if (strcmp(argv[1], "readwrite") == 0)
	{
		if (argc == 3)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			db = db_open(DATAS);

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;

			int n = rand() % 2;
			if (n)
			{
				_read_test((void *)&data);
			}
			else
			{
				_write_test((void *)&data);
			}
		}
		else if (argc == 4)
		{
			count = atoi(argv[2]);
			_print_header(count);
			_print_environment();

			db = db_open(DATAS);

			data_t data;
			data.count = count;
			data.r = 0;
			data.start = 0;
			data.stop = count - 1;

			if (atoi(argv[3]) != 0)
				data.r = 1;

			// It is up to system to decide an operation. Either read or write
			int n = rand() % 2;
			if (n)
			{
				_read_test((void *)&data);
			}
			else
			{
				_write_test((void *)&data);
			}
		}
		else if (argc == 5)
		{
			int total_count = atoi(argv[2]);
			_print_header(total_count);
			_print_environment();

			int r = 0;
			int thread = 0;

			if (atoi(argv[3]) != 0)
				r = 1;

			int threads = atoi(argv[4]);
			if (threads < 1)
			{
				printf("This can not be applyed. Threads input must be >= 1\n");
				exit(1);
			}
			if (threads > total_count)
			{
				printf("This can not be applyed. Threads input must <= Count input\n");
				exit(1);
			}
			if (threads >= 1)
				thread = 1;

			db = db_open(DATAS);

			first_read_start_time = LONG_MAX;
			first_write_start_time = LONG_MAX;
			total_write_count = 0;
			total_read_count = 0;
			pthread_mutex_init(&m_total_read_count, NULL);
			pthread_mutex_init(&m_total_write_count, NULL);
			found = 0;

			total_read_cost = 0;
			total_write_cost = 0;
			pthread_mutex_init(&m_total_write_cost, NULL);
			pthread_mutex_init(&m_total_read_cost, NULL);
			pthread_mutex_init(&m_found, NULL);

			int flag = 0; // an to count/threads den einai akeraios

			double poses_aithseis_to_ka8e_nhma = (double)total_count / threads;
			if (isInteger(poses_aithseis_to_ka8e_nhma) == 1)
				flag = 1;

			int count = (long int)floor(poses_aithseis_to_ka8e_nhma);

			pthread_t id[threads];
			data_t array_structs[threads];
			int i;
			int eginan_reads = 0;
			int eginan_writes = 0;

			for (i = 0; i < threads; i++)
			{
				// n = rand() % 2;

				if (rand() % 2)
				{ // writes
					eginan_writes = 1;
					array_structs[i].r = r;
					array_structs[i].wichThread = i;
					array_structs[i].thread = thread;
					array_structs[i].start = (int)count * i;
					array_structs[i].stop = (int)count * i + count - 1;

					if ((i == threads - 1) && (flag == 1))
						array_structs[i].stop = total_count - 1;

					pthread_create(&id[i], NULL, _write_test, (void *)&array_structs[i]);
				}
				else
				{ // reads
					eginan_reads = 1;
					array_structs[i].r = r;
					array_structs[i].thread = thread;
					array_structs[i].wichThread = i;
					array_structs[i].start = (int)count * i;
					array_structs[i].stop = (int)count * i + count - 1;

					if ((i == threads - 1) && (flag == 1))
						array_structs[i].stop = total_count - 1;

					pthread_create(&id[i], NULL, _read_test, (void *)&array_structs[i]);
				}
			}
			for (i = 0; i < threads; i++)
				pthread_join(id[i], NULL);

			db_close(db);
			total_write_cost = last_write_stop_time - first_write_start_time;
			total_read_cost = last_read_stop_time - first_read_start_time;

			if (eginan_reads)
				printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
					   total_read_count, found,
					   (double)(total_read_cost / total_read_count),
					   (double)(total_read_count / total_read_cost),
					   total_read_cost);
			if (eginan_writes)
				printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n", total_write_count, (double)(total_write_cost / total_write_count), (double)(total_write_count / total_write_cost), total_write_cost);
		}
	}
	else
	{
		fprintf(stderr, "Usage: db-bench <write | read | readwrite> <count> <random> <threads>\n");
		exit(1);
	}
	return 1;
}