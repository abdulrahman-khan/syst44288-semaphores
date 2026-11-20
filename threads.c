/*
 * SYST44288 
 * Zaid Al Sabbagh - 991643824
 * Abdulrahman Khan - 991542771
 * Petar Maslic - 991792427
 */
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "buffer.h"

#define MAX_SLEEP 3 

/* Shared state: locations and pointers for circular queue
    in = next slot to write
    out = slot to read from 
*/
buffer_item buffer[BUFFER_SIZE];
static int in = 0, out = 0; /* circular queue indices */

/* Sync logic 
    mutex enforces mutual exclusion
    empty/full are unnamed semaphores because 
    threads share one process's address space    
*/
static pthread_mutex_t mutex;
static sem_t empty, full;

/* inits mutex and semaphores
    full starts at 0 since nothing exists to consume
    empty starts at BUFFER_SIZE since every slot is free at the start
    0 passed to sem_init() marks a thread-shared, unnamed semaphore
    returns -1 if any of the three calls fail, 0 otherwise
*/
int buffer_init(void) {
    if (pthread_mutex_init(&mutex, NULL) != 0) return -1;
    if (sem_init(&empty, 0, BUFFER_SIZE) != 0) return -1;
    if (sem_init(&full, 0, 0) != 0) return -1;
    return 0;
}

/* used by producer to insert items into the buffer
    1. sem_wait(empty) blocks when buffer is full
    2. locks the mutex, moves to critical section
    3. write executed and advanced in, wrapping with BUFFER_SIZE
    4. unlock before signalling
    5. sem_post(full) tells consumer a slot is open
*/
int insert_item(buffer_item item) {
    if (sem_wait(&empty) != 0) return -1;
    if (pthread_mutex_lock(&mutex) != 0) return -1;
    buffer[in] = item;
    in = (in + 1) % BUFFER_SIZE;
    if (pthread_mutex_unlock(&mutex) != 0) return -1;
    if (sem_post(&full) != 0) return -1;
    return 0;
}

/* Used by consumer to remove items from queue
    1. sem_wait(full) blocks when buffer is empty
    2. locks the mutex, moves to critical section
    3. read executed and advanced out, wrapping with BUFFER_SIZE
    4. unlock before signalling
    5. sem_post(empty) tells producer a slot is free again
*/
int remove_item(buffer_item *item) {
    if (sem_wait(&full) != 0) return -1;
    if (pthread_mutex_lock(&mutex) != 0) return -1;
    *item = buffer[out];
    out = (out + 1) % BUFFER_SIZE;
    if (pthread_mutex_unlock(&mutex) != 0) return -1;
    if (sem_post(&empty) != 0) return -1;
    return 0;
}

/* Produces items into queue
    1. sleeps
    2. sets random value to 'item'
    3. insert_item(item) inserts item into queue
    4. prints success message
*/
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

/* Consumes items from queue
    1. sleeps
    2. remove_item(&item) removes item from queue
    3. value returns through the pointer into 'item'
    4. prints success message
*/
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

/* usage statistics */
static void usage(const char *prog) {
    fprintf(stderr, "usage: %s <sleep_time> <num_producers> <num_consumers>\n", prog);
    exit(EXIT_FAILURE);
}

/* starts worker threads
    1. loops 'count' times calling pthread_create with 'fn'
    2. pthread_create returns an error number instead of setting errno
    3. prints the reason with strerror and exits if a thread fails to start
*/
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

/* main
    1. checks there are exactly 3 arguments in the order the spec sets
    2. parses sleep time, producer count and consumer count
    3. buffer_init() creates the mutex and the two semaphores
    4. srand seeds rand with the current time so runs differ
    5. spawns the producers, then the consumers
    6. sleeps for sleep_time, then returning from main ends every thread
*/
int main(int argc, char *argv[]) {
    if (argc != 4) usage(argv[0]);
    int sleep_time = atoi(argv[1]);
    int num_producers = atoi(argv[2]);
    int num_consumers = atoi(argv[3]);

    buffer_init();
    srand((unsigned)time(NULL));

    spawn(producer, num_producers, "producer");
    spawn(consumer, num_consumers, "consumer");

    sleep(sleep_time);
    return EXIT_SUCCESS;
}
