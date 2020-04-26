#include <cassandra.h>
#include <unistd.h>
#include <stdio.h>	/* for printf */
#include <stdint.h>	/* for uint64 definition */
#include <stdlib.h>	/* for exit() definition */
#include <time.h>	/* for clock_gettime */
#include <pthread.h> 


#define BILLION 1000000000L

void on_result(CassFuture* result_future, void* data) {
    struct timespec *start_p = (struct timespec*)data;
    struct timespec start = *start_p;
    struct timespec end;
    uint64_t diff;



    if (cass_future_error_code(result_future) == CASS_OK) {
      clock_gettime(CLOCK_MONOTONIC, &end);

      diff = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
      printf("%llu\n", (long long unsigned int) diff);
      /* Retrieve result set and get the first row */
/*      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "name");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length, release_version);


      }

      cass_result_free(result);*/
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }

    free(start_p);
}

void *read_thread(void *vargp) {

      CassSession* session = (CassSession*)vargp;
//      int i = *(int*)vargp;

//      printf("%d\n", i);

      /* Build statement and execute query */
//    const char* query = "SELECT release_version FROM system.local";
    int i = 0;
    for (i = 0; i < 300000; i++) {
      const char* query = "SELECT name FROM mittcpu.students WHERE id = 6";
      CassStatement* statement = cass_statement_new(query, 0);
      struct timespec *start = malloc(sizeof(struct timespec));
      clock_gettime(CLOCK_MONOTONIC, start);


      CassFuture* result_future = cass_session_execute(session, statement);

      cass_future_set_callback(result_future, on_result, (void*)start);

      cass_statement_free(statement);
      cass_future_free(result_future);
      usleep(1000);
    }
}


void *connect_thread(void *vargp) {
  printf("starting a new session...\n");

  int n_thread = 10;
  pthread_t* tid = malloc(sizeof(pthread_t) * n_thread);
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
//  char* hosts = "heavy-client.cass-5n.ucare.emulab.net";
  char* hosts = "155.98.36.37";
  char* whilelist_hosts = "155.98.36.37";


  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  cass_cluster_set_whitelist_filtering(cluster, whilelist_hosts);

//  cass_cluster_set_num_threads_io(cluster, 1);

//  cass_cluster_set_core_connections_per_host(cluster, 2);


  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    int i;
//    int n_thread = 10;
    for (i = 0; i < n_thread; i++)
        pthread_create(&tid[i], NULL, read_thread, (void *)session);
    int j;
    for (j = 0; j < n_thread; j++)
        pthread_join(tid[j], NULL);

  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
}



int main(int argc, char* argv[]) {
  int n_thread = 5;
  pthread_t* tid = malloc(sizeof(pthread_t) * n_thread);
  int i;
    for (i = 0; i < n_thread; i++)
        pthread_create(&tid[i], NULL, connect_thread, NULL);
    int j;
    for (j = 0; j < n_thread; j++)
        pthread_join(tid[j], NULL);

  return 0;
}
