#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "buffer.h"

#define MAX_SLEEP 3 /* seconds a producer/consumer naps between operations */

buffer_item buffer[BUFFER_SIZE];
static int in = 0, out = 0; /* circular queue indices */

static pthread_mutex_t mutex;
static sem_t empty, full;

int buffer_init(void) {
    if (pthread_mutex_init(&mutex, NULL) != 0) return -1;
    if (sem_init(&empty, 0, BUFFER_SIZE) != 0) return -1;
    if (sem_init(&full, 0, 0) != 0) return -1;
    return 0;
}

int insert_item(buffer_item item) {
    if (sem_wait(&empty) != 0) return -1;
    if (pthread_mutex_lock(&mutex) != 0) return -1;
    buffer[in] = item;
    in = (in + 1) % BUFFER_SIZE;
    if (pthread_mutex_unlock(&mutex) != 0) return -1;
    if (sem_post(&full) != 0) return -1;
    return 0;
}

int remove_item(buffer_item *item) {
    if (sem_wait(&full) != 0) return -1;
    if (pthread_mutex_lock(&mutex) != 0) return -1;
    *item = buffer[out];
    out = (out + 1) % BUFFER_SIZE;
    if (pthread_mutex_unlock(&mutex) != 0) return -1;
    if (sem_post(&empty) != 0) return -1;
    return 0;
}

void *producer(void *param) {
    (void)param;
    buffer_item item;
    while (true) {
        sleep(rand() % MAX_SLEEP + 1);
        item = rand();
        if (insert_item(item))
            fprintf(stderr, "error: producer failed to insert item\n");
        else
            printf("Producer produced %d\n", item);
    }
}

void *consumer(void *param) {
    (void)param;
    buffer_item item;
    while (true) {
        sleep(rand() % MAX_SLEEP + 1);
        if (remove_item(&item))
            fprintf(stderr, "error: consumer failed to remove item\n");
        else
            printf("Consumer consumed %d\n", item);
    }
}

static void usage(const char *prog) {
    fprintf(stderr, "usage: %s <sleep_time> <num_producers> <num_consumers>\n", prog);
    exit(EXIT_FAILURE);
}

/* Strict positive-integer parse: rejects "", "abc", "5x", "-1", "0", overflow. */
static int parse_positive(const char *s, const char *prog) {
    char *end;
    errno = 0;
    long v = strtol(s, &end, 10);
    if (errno != 0 || end == s || *end != '\0' || v <= 0 || v > INT_MAX) usage(prog);
    return (int)v;
}

static void spawn(void *(*fn)(void *), int count, const char *role) {
    for (int i = 0; i < count; i++) {
        pthread_t tid;
        int err = pthread_create(&tid, NULL, fn, NULL);
        if (err != 0) {
            fprintf(stderr, "error: failed to create %s thread: %s\n", role, strerror(err));
            exit(EXIT_FAILURE);
        }
    }
}

int main(int argc, char *argv[]) {
    if (argc != 4) usage(argv[0]);
    int sleep_time = parse_positive(argv[1], argv[0]);
    int num_producers = parse_positive(argv[2], argv[0]);
    int num_consumers = parse_positive(argv[3], argv[0]);

    if (buffer_init() != 0) {
        perror("error: failed to initialize buffer");
        return EXIT_FAILURE;
    }
    srand((unsigned)time(NULL));

    spawn(producer, num_producers, "producer");
    spawn(consumer, num_consumers, "consumer");

    sleep(sleep_time);
    return EXIT_SUCCESS; /* returning from main ends the process and all threads */
}
