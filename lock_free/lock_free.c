#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <threads.h>
#include <time.h>

typedef struct node {
  int value;
  struct node *next;
} node;
typedef struct queue {
  node *head;
  node *tail;
  size_t size;
  pthread_mutex_t mtx;
} queue;

typedef struct start_arg {
  queue *q;
  int i;
  int k;
} start_arg;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

void init_queue(queue *q) {
  q->head = q->tail = NULL;
  q->size = 0;
  pthread_mutex_init(&q->mtx, NULL);
}
void delete_queue(queue *q) {
  if (q->head == NULL) {
    printf("Queue already empty\n");

    return;
  }

  node *n = q->head;
  while (n->next != NULL) {
    node *next = n->next;
    free(n);
    n = next;
  }
  pthread_mutex_destroy(&q->mtx);
}

int enqueue(queue *q, int v) {
  node *n = (node *)malloc(sizeof(*n));
  n->value = v;
  n->next = NULL;
  pthread_mutex_lock(&q->mtx);

  if (q->tail) {
    q->tail->next = n;
    q->tail = n;

  } else {
    // First element of the list
    q->head = q->tail = n;
  }
  q->size++;
  pthread_mutex_unlock(&q->mtx);
  return 0;
}

int dequeue(queue *q, int *out) {
  pthread_mutex_lock(&q->mtx);

  node *n = q->head;
  if (!n) {
    // Queue is empty
    pthread_mutex_unlock(&q->mtx);
    return 1;
  }

  q->head = n->next;
  if (q->head == NULL)
    q->tail = NULL;
  q->size--;
  pthread_mutex_unlock(&q->mtx);
  *out = n->value;
  free(n);
  return 0;
}

void *start_routine(void *_arg) {
  start_arg *a = (start_arg *)(_arg);
  int i = a->i;
  int k = a->k;
  queue *q = a->q;
  // printf("[%llx] Hello from thread %d\n, adding this number to
  // queue...\n",(unsigned long long)pthread_self(), i);

  for (int j = 0; j < k; j++) {
    enqueue(q, i);
  }
  for (int j = 0; j < k; j++) {
    dequeue(q, &i);
  }

  return NULL;
}

/*
Lock free queue
*/

typedef struct mc_node {
  int value;
  _Atomic(struct node *) next;
} ms_node;

typedef struct mc_queue {
  _Atomic(ms_node *) head;
  _Atomic(ms_node *) tail;
} ms_queue;

void init_ms_queue(ms_queue *q) { q->head = q->tail = NULL; }

int ms_enqueue(ms_queue *q, int v) {
  ms_node *n = (ms_node *)malloc(sizeof(*n));
  n->value = v;
  atomic_init(&n->next, NULL);

  while (1) {
    ms_node *t = atomic_load_explicit(&q->tail, memory_order_acquire);
    ms_node *tn = atomic_load_explicit(&t->next, memory_order_acquire);

    if (tn == NULL) {
      if (atomic_compare_exchange_strong_explicit(
              &t->next, &tn, n, memory_order_release, memory_order_acquire)) {
        atomic_compare
      }
    }
  }

  // If the tail->next is null
  if (!(q->tail->next)) {
  }

  return 0;
}

int main(int argc, char *argv[]) {
  int n = atoi(argv[1]);
  int k = atoi(argv[2]);
  void *retval;
  pthread_t *tid = malloc(n * sizeof(pthread_t));

  queue q;
  init_queue(&q);
  for (int i = 0; i < n; i++) {
    start_arg *a = malloc(sizeof(*a));
    a->i = i;
    a->q = &q;
    a->k = k;
    pthread_create(&tid[i], NULL, start_routine, a);
  }

  for (int i = 0; i < n; i++) {
    printf("Joining thread %d\n", i);
    pthread_join(tid[i], &retval);
  }

  printf("Success\n");

  printf("Queue length: %d\n", (int)q.size);
  // node *node = q.head;
  // for (int i = 0; i < q.size; i++) {
  //   printf("Value in node: %d\n", node->value);
  //   node = node->next;
  // }
  delete_queue(&q);
  return 0;
}
