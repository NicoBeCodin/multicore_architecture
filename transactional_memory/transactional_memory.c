#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>

// 1024*1024
#define NB_CELLS 65536

static pthread_mutex_t g_mutex;

static inline int64_t timespec_to_ns(struct timespec ts) {
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

static inline int64_t now_ns(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    // handle error if you want: perror("clock_gettime");
    return -1;
  }
  return timespec_to_ns(ts);
}

typedef struct cell {
  _Atomic uintptr_t value;
  size_t counter;
} cell;

typedef struct memory {
  cell *cells[NB_CELLS];
  size_t clock;
} memory;

void init_mem(memory *m) {
  m->clock = 0;
  cell *pool = calloc(NB_CELLS, sizeof *pool);
  printf("First value of pool for init: %d %d \n", (int)pool[0].value,
         (int)pool[0].counter);
  for (int i = 0; i < NB_CELLS; i++) {
    m->cells[i] = &pool[i];
  }
  printf("Succesfully intialized mem\n");
}

typedef struct start_arg {
  int i;
  memory *m;
} start_arg;

void *start_routine_atomic(void *arg) {
  start_arg *start = (start_arg *)(arg);
  memory *m = start->m;
  printf("[%llx] Hello from thread %d\n", (unsigned long long)pthread_self(),
         start->i);
  for (int i = 0; i < NB_CELLS; i++) {
    atomic_fetch_add_explicit((&m->cells[i]->value), (uintptr_t)1,
                              memory_order_relaxed);
  }
  return NULL;
}

int check_cells(int expected, memory *m) {
  for (int i = 0; i < NB_CELLS; i++) {
    if (m->cells[i]->value != expected) {
      printf("Cell %d has %d as value, expected: %d\n", i,
             (int)m->cells[i]->value, expected);
      return 1;
    }
  }
  printf("Successfully checked cells\n");
  return 0;
}

void *start_routine_mutex(void *arg) {
  start_arg *start = (start_arg *)(arg);
  memory *m = start->m;
  printf("[%llx] Hello from thread %d\n", (unsigned long long)pthread_self(),
         start->i);

  pthread_mutex_lock(&g_mutex);
  for (int i = 0; i < NB_CELLS; i++) {
    m->cells[i]->value += 1;
  }
  pthread_mutex_unlock(&g_mutex);
  return NULL;
}

/*
transactional memory
*/

/*
hazard pointer management
*/

#define MAX_THREADS 128
#define HP_SLOTS 2
#define RETIRE_BATCH 64

typedef void (*hp_free_fn)(void *);

typedef struct {
  void *ptr;
  hp_free_fn freer;
} hp_retired;

typedef struct {
  int tid;
  _Atomic(void *) *hpbase;
  hp_retired *retired;
  size_t rcap, rlen;
} hp_thread;

extern _Atomic(void *) g_hazards[MAX_THREADS * HP_SLOTS];

static inline void hp_global_init(void) {
  for (int i = 0; i < MAX_THREADS * HP_SLOTS; ++i) {
    atomic_store_explicit(&g_hazards[i], NULL, memory_order_relaxed);
  }
}

static inline void hp_thread_init(hp_thread *ht, int tid) {
  ht->tid = tid;
  ht->hpbase = g_hazards;
  ht->rcap = RETIRE_BATCH;
  ht->rlen = 0;
  ht->retired = (hp_retired *)malloc(ht->rcap * sizeof(*ht->retired));
}

static inline void hp_set(hp_thread *ht, int slot, void *p) {
  atomic_store_explicit(&ht->hpbase[ht->tid * HP_SLOTS + slot], p,
                        memory_order_seq_cst);
}

static inline void hp_clear(hp_thread *ht, int slot) {
  atomic_store_explicit(&ht->hpbase[ht->tid * HP_SLOTS + slot], NULL,
                        memory_order_seq_cst);
}

static inline void hp_retire(hp_thread *ht, void *p, hp_free_fn f) {
  if (ht->rlen == ht->rcap) {
    ht->rcap *= 2;
    ht->retired =
        (hp_retired *)realloc(ht->retired, ht->rcap * sizeof(*ht->retired));
  }
  ht->retired[ht->rlen++] = (hp_retired){p, f};
}

static inline void hp_scan_and_reclaim(hp_thread *ht) {
  // Snapshot hazards
  size_t hlen = MAX_THREADS * HP_SLOTS;
  void **snap = (void **)malloc(hlen * sizeof(*snap));
  for (size_t i = 0; i < hlen; ++i) {
    snap[i] = atomic_load_explicit(&ht->hpbase[i], memory_order_seq_cst);
  }

  size_t w = 0;
  for (size_t r = 0; r < ht->rlen; ++r) {
    void *p = ht->retired[r].ptr;
    bool hazard = false;
    for (size_t i = 0; i < hlen; ++i) {
      if (snap[i] == p) {
        hazard = true;
        break;
      }
    }
    if (hazard) {
      ht->retired[w++] = ht->retired[r];
    } else {
      ht->retired[r].freer(p);
    }
  }
  ht->rlen = w;
  free(snap);
}

static inline void hp_thread_destroy(hp_thread *ht) {
  hp_scan_and_reclaim(ht);
  for (size_t r = 0; r < ht->rlen; r++) {
    ht->retired[r].freer(ht->retired[r].ptr);
  }
  free(ht->retired);
}

_Atomic(void *) g_hazards[MAX_THREADS * HP_SLOTS];

static _Thread_local hp_thread tls_hp;
static _Thread_local bool hp_inited = false;

static inline void tm_register_if_needed(int tid) {
  if (!hp_inited) {
    hp_thread_init(&tls_hp, tid);
    hp_inited = true;
  }
}

void startTX();
uintptr_t read(int n);
void write(int n, uintptr_t value);
bool commitTX();

typedef struct {
  _Atomic(cell *) cells[NB_CELLS];
  _Atomic size_t clock;
  pthread_mutex_t commit_lock;
} tm_memory;

typedef struct {
  int idx;
} REntry;
typedef struct {
  int idx;
  uintptr_t val;
} WEntry;

#define MAX_RSET 1024
#define MAX_WSET 1024
#define MAX_INT 64

typedef struct {
  size_t clock;
  REntry rset[MAX_RSET];
  int rcount;
  WEntry wset[MAX_WSET];
  int wcount;
  bool aborted;
} Tx;

static _Thread_local Tx tx;
tm_memory gmem;

void init_tx_memory(tm_memory *m) {
  pthread_mutex_init(&m->commit_lock, NULL);
  atomic_store_explicit(&m->clock, 0, memory_order_relaxed);
  for (size_t i = 0; i < NB_CELLS; ++i) {
    cell *c = (cell *)malloc(sizeof *c);
    c->value = 0;
    c->counter = 0;
    atomic_store_explicit(&m->cells[i], c, memory_order_relaxed);
  }
}

void startTX(void) {
  tx.rcount = tx.wcount = 0;
  tx.aborted = false;
  tx.clock = atomic_load_explicit(&gmem.clock, memory_order_acquire) + 1;
}

uintptr_t read(int n) {

  // Return a deferred write if present
  for (int i = 0; i < tx.wcount; ++i) {
    if (tx.wset[i].idx == n)
      return tx.wset[i].val;
  }
  for (;;) {
    cell *p = atomic_load_explicit(&gmem.cells[n], memory_order_acquire);

    hp_set(&tls_hp, 0, p); // We publish a hazard pointer

    cell *q = atomic_load_explicit(&gmem.cells[n], memory_order_acquire);

    if (p != q) {
      // lost the race, clear and retry
      hp_clear(&tls_hp, 0);
      continue;
    }
    uintptr_t v = p->value;
    size_t ctr = p->counter;

    if (ctr >= tx.clock)
      tx.aborted = true;
    else if (tx.rcount < MAX_RSET)
      tx.rset[tx.rcount++].idx = n;
    else
      tx.aborted = true;

    hp_clear(&tls_hp, 0);
    return v;
  }
}

void write(int n, uintptr_t value) {

  for (int i = 0; i < tx.wcount; ++i) {
    if (tx.wset[i].idx == n) {
      tx.wset[i].val = value;
      return;
    }
  }
  if (tx.wcount < MAX_WSET) {
    tx.wset[tx.wcount++] = (WEntry){n, value};

  } else {
    tx.aborted = true;
  }
}

static inline bool validate_reads_locked(void) {
  for (int i = 0; i < tx.rcount; ++i) {
    int idx = tx.rset[i].idx;
    cell *c = atomic_load_explicit(&gmem.cells[idx], memory_order_acquire);
    if (c->counter >= tx.clock)
      return false;
  }
  return true;
}

bool commitTX(void) {
  if (tx.aborted)
    return false;

  pthread_mutex_lock(&gmem.commit_lock);

  if (!validate_reads_locked()) {
    pthread_mutex_unlock(&gmem.commit_lock);
    return false;
  }
  size_t cur_clock = atomic_load_explicit(&gmem.clock, memory_order_relaxed);

  for (int i = 0; i < tx.wcount; ++i) {
    int idx = tx.wset[i].idx;
    // Allocate new immutable cell
    cell *newc = (cell *)malloc(sizeof(*newc));
    newc->counter = cur_clock;
    newc->value = tx.wset[i].val;

    cell *old = atomic_load_explicit(&gmem.cells[idx], memory_order_acquire);
    atomic_store_explicit(&gmem.cells[idx], newc, memory_order_release);

    hp_retire(&tls_hp, old, free);
  }
  atomic_store_explicit(&gmem.clock, cur_clock + 1, memory_order_release);
  pthread_mutex_unlock(&gmem.commit_lock);

  if (tls_hp.rlen >= RETIRE_BATCH)
    hp_scan_and_reclaim(&tls_hp);

  return true;
}

void test_tm() {
  for (int i = 0; i < NB_CELLS; i++) {
  restart:
    startTX();
    int value = read(i); // read x
    if (value == MAX_INT)
      continue;
    write(i, value + 1);
    if (!commitTX())
      goto restart;
  }
}

void *start_routine_tm(void *_arg) {
  // the thread number
  int *k = (int *)_arg;
  tm_register_if_needed(*k);
  printf("[%llx] Hello from thread %d\n", (unsigned long long)pthread_self(),
         *k);
  test_tm();
  hp_thread_destroy(&tls_hp);
  return NULL;
}

void check_tm_cells( tm_memory *mem, int n) {
  for (int i = 1; i < NB_CELLS; i*=2) {
    cell* c = atomic_load(&mem->cells[i]);
    int v = (int)atomic_load(&(c->value));
    
    printf("Cell %d : \ncounter: %d \nvalue: %d\n\n", i,(int)c->counter, v);
    if (v!=n){
      printf("Wrong value for cell %d\n", i);
    }
  }
  
}

int main(int argc, char *argv[]) {
  int n = atoi(argv[1]);
  int choice = atoi(argv[2]);

  void *retval;
  pthread_t *tid = malloc(n * sizeof(pthread_t));
  memory mem;
  int64_t start, end;
  init_mem(&mem);

  if (choice == 1) {
    // ATOMIC
    start = now_ns();
    for (int i = 0; i < n; i++) {
      start_arg *p = malloc(sizeof(*p));
      p->i = i;
      p->m = &mem;
      pthread_create(&tid[i], NULL, start_routine_atomic, p);
    }

    for (int i = 0; i < n; i++) {
      pthread_join(tid[i], &retval);
    }

    end = now_ns();
  } else if (choice == 2) {
    // MUTEX
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutex_init(&g_mutex, &attr);
    pthread_mutexattr_destroy(&attr);

    start = now_ns();
    for (int i = 0; i < n; i++) {
      start_arg *p = malloc(sizeof(*p));
      p->i = i;
      p->m = &mem;
      pthread_create(&tid[i], NULL, start_routine_mutex, p);
    }

    for (int i = 0; i < n; i++) {
      pthread_join(tid[i], &retval);
    }
    end = now_ns();
  } else if (choice == 3) {
    // Transactional memory
    init_tx_memory(&gmem);
    hp_global_init();

    start = now_ns();
    for (int i = 0; i < n; i++) {
      start_arg *p = malloc(sizeof(*p));
      p->i = i;
      p->m = &mem;
      pthread_create(&tid[i], NULL, start_routine_tm, p);
    }

    for (int i = 0; i < n; i++) {
      pthread_join(tid[i], &retval);
    }
    check_tm_cells(&gmem, n);
    end = now_ns();
  } else {
    printf("Wrong arg: %d\n", choice);
    return -1;
  }

  printf("Exec time: %ld ns\n", end - start);
  printf("Successfully joined threads\n");
  check_cells(n, &mem);
  pthread_exit(NULL);
  return 0;
}
