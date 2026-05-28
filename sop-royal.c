#define _GNU_SOURCE
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <time.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>

#define ERR(source) (perror(source), fprintf(stderr, "%s:%d\n", __FILE__, __LINE__), exit(EXIT_FAILURE))

#define MAX_EVENTS 64
#define MAX_MSG_LEN 512

// Global signal flag
volatile sig_atomic_t keep_running = 1;

void sigint_handler(int signum) {
    keep_running = 0;
}

// Data structures
typedef struct {
    int urgency;
    char town[33];
    char report[129];
} report_t;

typedef struct {
    report_t heap[MAX_EVENTS];
    int size;
    int terminate;
    pthread_mutex_t mutex;
    pthread_cond_t king_wakeup;
} strategy_room_t;

typedef struct {
    char buffer[MAX_MSG_LEN];
    strategy_room_t *room;
} worker_arg_t;

// Initialization and cleanup
void setup_signals(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigint_handler;
    if (sigaction(SIGINT, &sa, NULL) < 0) ERR("sigaction"); 
}

void init_strategy_room(strategy_room_t *room) {
    room->size = 0;
    room->terminate = 0;
    if (pthread_mutex_init(&room->mutex, NULL) != 0) ERR("pthread_mutex_init");
    if (pthread_cond_init(&room->king_wakeup, NULL) != 0) ERR("pthread_cond_init");
}

void cleanup_strategy_room(strategy_room_t *room) {
    pthread_mutex_destroy(&room->mutex);
    pthread_cond_destroy(&room->king_wakeup);
}

// Heap logic
void swap_reports(report_t *a, report_t *b) {
    report_t temp = *a;
    *a = *b;
    *b = temp;
}

void heap_insert(strategy_room_t *room, report_t new_report) {
    if (room->size >= MAX_EVENTS) return; 

    int curr = room->size;
    room->heap[curr] = new_report;
    room->size++;

    while (curr > 0) {
        int parent = (curr - 1) / 2;
        if (room->heap[parent].urgency >= room->heap[curr].urgency) {
            break;
        }
        swap_reports(&room->heap[parent], &room->heap[curr]);
        curr = parent;
    }
}

report_t heap_extract_max(strategy_room_t *room) {
    report_t max_report = room->heap[0];
    
    room->heap[0] = room->heap[room->size - 1];
    room->size--;

    int curr = 0;
    while ((2 * curr) + 1 < room->size) {
        int left = (2 * curr) + 1;
        int right = (2 * curr) + 2;
        int largest = curr;

        if (room->heap[left].urgency > room->heap[largest].urgency) {
            largest = left;
        }
        if (right < room->size && room->heap[right].urgency > room->heap[largest].urgency) {
            largest = right;
        }
        if (largest == curr) {
            break;
        }

        swap_reports(&room->heap[curr], &room->heap[largest]);
        curr = largest;
    }

    return max_report;
}

// Socket setup
int make_udp_socket(int port) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) ERR("socket");

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) ERR("bind");
    return fd;
}

// Worker thread
void *worker_thread(void *arg) {
    pthread_detach(pthread_self());
    
    worker_arg_t *args = (worker_arg_t *)arg;
    char *buffer = args->buffer;
    strategy_room_t *room = args->room;

    char *saveptr;
    char *u_str = strtok_r(buffer, "|", &saveptr);
    char *t_str = strtok_r(NULL, "|", &saveptr);
    char *r_str = strtok_r(NULL, "\r\n", &saveptr);

    if (!u_str || !t_str || !r_str || strlen(t_str) > 32 || strlen(r_str) > 128) {
        fprintf(stderr, "Error: Malformed message\n");
        free(args);
        return NULL;
    }

    int urgency = atoi(u_str);
    if (urgency < 1 || urgency > 100) {
        fprintf(stderr, "Error: Urgency out of bounds\n");
        free(args);
        return NULL;
    }

    report_t new_report;
    new_report.urgency = urgency;
    strncpy(new_report.town, t_str, 32);
    new_report.town[32] = '\0';
    strncpy(new_report.report, r_str, 128);
    new_report.report[128] = '\0';

    pthread_mutex_lock(&room->mutex);
    heap_insert(room, new_report);
    pthread_mutex_unlock(&room->mutex);

    free(args);
    return NULL;
}

// King thread
void *king_thread_work(void *arg) {
    strategy_room_t *room = (strategy_room_t *)arg;

    pthread_mutex_lock(&room->mutex);

    while (1) {
        if (room->terminate) {
            while (room->size > 0) {
                report_t r = heap_extract_max(room);
                printf("[URGENCY: %d] Message from %s: %s\n", r.urgency, r.town, r.report);
            }
            break;
        }

        if (room->size > 0) {
            report_t r = heap_extract_max(room);
            printf("[URGENCY: %d] Message from %s: %s\n", r.urgency, r.town, r.report);
        }

        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 3; 
        
        pthread_cond_timedwait(&room->king_wakeup, &room->mutex, &ts);
    }

    pthread_mutex_unlock(&room->mutex);
    return NULL;
}

// Core server loop
void run_server(int fd, strategy_room_t *room) {
    char temp_buffer[MAX_MSG_LEN];

    while (keep_running) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);

        ssize_t n = recvfrom(fd, temp_buffer, sizeof(temp_buffer) - 1, 0, (struct sockaddr *)&client_addr, &client_len);
        
        if (n < 0) {
            if (errno == EINTR) {
                break; 
            }
            ERR("recvfrom");
        }
        temp_buffer[n] = '\0';

        worker_arg_t *w_args = malloc(sizeof(worker_arg_t));
        if (!w_args) ERR("malloc");
        
        strcpy(w_args->buffer, temp_buffer);
        w_args->room = room;

        pthread_t worker_tid;
        if (pthread_create(&worker_tid, NULL, worker_thread, w_args) != 0) ERR("pthread_create");
    }
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "USAGE: %s port\n", argv[0]);
        return EXIT_FAILURE;
    }

    int port = atoi(argv[1]);
    
    setup_signals();
    
    strategy_room_t room;
    init_strategy_room(&room);

    int fd = make_udp_socket(port);

    pthread_t king_tid;
    if (pthread_create(&king_tid, NULL, king_thread_work, &room) != 0) ERR("pthread_create");

    run_server(fd, &room);

    pthread_mutex_lock(&room.mutex);
    room.terminate = 1;
    pthread_cond_signal(&room.king_wakeup); 
    pthread_mutex_unlock(&room.mutex);

    pthread_join(king_tid, NULL);

    if (close(fd) < 0) ERR("close");
    cleanup_strategy_room(&room);

    return EXIT_SUCCESS;
}