WARN=-Wall -Wno-implicit-function-declaration -Wno-unused-but-set-variable
DEBUG=-g -ggdb

CFLAGS += -g

all:
	gcc $(DEBUG) $(WARN) bench.c -lm kiwi.c -L ../engine -lindexer -lpthread -lsnappy -o kiwi-bench
clean:
	rm -f kiwi-bench
	rm -rf testdb
