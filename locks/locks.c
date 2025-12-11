
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define asm_pause() __asm__ __volatile__("pause")

uint32_t z = 0;

uint32_t n, it, csd, cd;

void (*ze_lock)();
void (*ze_unlock)();
/*

Spin lock
Good if rarely locked, and almost always free but very ineeficient in other
cases The cache line that holds the lock variable continuously bounces between
the cores
*/
static atomic_int slock = ATOMIC_VAR_INIT(0);
uint32_t _Atomic spinlock = 0;

void spin_lock(void) {
  while (atomic_exchange(&spinlock, 1)) {
    asm_pause();
  }
}

void spin_unlock(void) { atomic_store(&spinlock, 0); }

/*
POSIX lock
*/
static pthread_mutex_t g_mutex;
static void posix_mutex_lock(void) { pthread_mutex_lock(&g_mutex); }
static void posix_mutex_unlock(void) { pthread_mutex_unlock(&g_mutex); }

/*
TICKET LOCK
*/
struct {
  uint32_t _Atomic counter;
  uint32_t _Atomic screen;
  
} ticket = {0,0};

void ticket_lock(){
  uint32_t my = atomic_fetch_add(&ticket.counter,1);
  while(atomic_load(&ticket.screen)<my) {
    asm_pause();
    
  }
}

void ticket_unlock(){
  atomic_fetch_add(&ticket.screen, 1);
}

/*
Benchmark
*/

struct {
  pthread_t tid;
  double elapsed;
} *stats;

int _Atomic nb_started = 0;

double gettime() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ts.tv_sec * 1e0 + ts.tv_nsec * 1e-9;
}

double delay(double from, uint32_t d) {
  if (d) {
    double res;
    do {
      res = gettime();
    } while (res < from + d * 1e-9);
    return res;
  } else
    return from;
}

void *f(void *_arg) {
  uint32_t self = (uint32_t)(uintptr_t)_arg;
  atomic_fetch_add(&nb_started, 1);
  while (atomic_load(&nb_started) < n) {
    asm_pause();
  }
  double start = gettime();
  double cur = start;
  for (int i = 0; i < it; i++) {
    ze_lock();
    z++;
    cur = delay(cur, csd);
    ze_unlock();
    cur = delay(cur, cd);
  }
  stats[self].elapsed = gettime() - start;
  return 0;
}

int main(int argc, char *argv[]) {
  if (argc < 6) {
    fprintf(stderr, "Usage: %s nb-threads it csd cd algo\n", argv[0]);
    exit(1);
  }

  n = (uint32_t)atoi(argv[1]);
  it = (uint32_t)atoi(argv[2]);
  csd = (uint32_t)atoi(argv[3]);
  cd = (uint32_t)atoi(argv[4]);
  char *algo = argv[5];

  stats = calloc(n, sizeof(*stats));
  printf("Testing with %s lock \n", algo);
  if (strcmp(algo, "spinlock") == 0) {
    ze_lock = spin_lock;
    ze_unlock = spin_unlock;
  } else if (strcmp(algo, "posix") == 0) {
    ze_lock = posix_mutex_lock;
    ze_unlock = posix_mutex_unlock;
  }else if (strcmp(algo,"ticket" )==0){
      ze_lock = ticket_lock;
      ze_unlock = ticket_unlock;
  } else {
    fprintf(stderr, "unknown lock %s\n", algo);
    exit(1);
  }

  pthread_mutexattr_t attr;
  pthread_mutexattr_init(&attr);
  pthread_mutex_init(&g_mutex, &attr);
  pthread_mutexattr_destroy(&attr);

  for (int i = 0; i < n; i++) {
    pthread_create(&stats[i].tid, NULL, f, (void *)(uintptr_t)i);
  }

  for (int i = 0; i < n; i++) {
    void *retval;
    pthread_join(stats[i].tid, &retval);
  }

  pthread_mutex_destroy(&g_mutex);
  double total = 0;
  for (int i = 0; i < n; i++) {
    total += stats[i].elapsed;
  }

  double av = ((total * 1e9) / (n * it)) - cd;
  printf("Average: %0.0lf ns per loop (with a delay if %u ns => %0.0lfns)\n",
         av, csd, av - csd);

  if (z != n * it) {
    printf("Integrity check failed: %u for %u\n", z, n * it);
  }

  return 0;
}
