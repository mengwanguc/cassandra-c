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
    int failover_count;
    int server_id[3];


    if (cass_future_error_code_mittcpu(result_future, &failover_count, server_id) == CASS_OK) {
      clock_gettime(CLOCK_MONOTONIC, &end);

      diff = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
      printf("%llu  +%d  -%d--%d---%d\n", (long long unsigned int) diff, failover_count,
             server_id[0], server_id[1], server_id[2]);
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

      // printf("Main Enter read_thread ++++\n");
      CassSession* session = (CassSession*)vargp;
//      int i = *(int*)vargp;

//      printf("%d\n", i);

      /* Build statement and execute query */
//    const char* query = "SELECT release_version FROM system.local";
    int i = 0;
    // printf("Main read_thread before LOOP\n");
    for (i = 0; i < 32500; i++) {
      const char* query = "SELECT name FROM cassDB.users WHERE id = 0df218dd-10fa-11ea-bf01-54271e04ce91";
      CassStatement* statement = cass_statement_new(query, 0);
      cass_statement_set_is_idempotent(statement, cass_true);
      struct timespec *start = malloc(sizeof(struct timespec));
      clock_gettime(CLOCK_MONOTONIC, start);


      CassFuture* result_future = cass_session_execute(session, statement);

      cass_future_set_callback(result_future, on_result, (void*)start);

      cass_statement_free(statement);
      cass_future_free(result_future);
      usleep(2000);
      // usleep(2000);
      // usleep(2000000);
    }
}


int main(int argc, char* argv[]) {
  int n_thread = 20;
  pthread_t* tid = malloc(sizeof(pthread_t) * n_thread);
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  // printf("Main before declaring the hosts\n");
//  char* hosts = "heavy-client.cass-5n.ucare.emulab.net";
  char* hosts = argv[1];
  char* whilelist_hosts = argv[1];

  // printf("Main before cass_cluster_set_contact_points\n");

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  cass_int64_t constant_delay_ms = 3;

  int max_speculative_executions = 1;

  cass_cluster_set_constant_speculative_execution_policy(cluster,
                                                       constant_delay_ms,
                                                       max_speculative_executions);

//  cass_cluster_set_whitelist_filtering(cluster, whilelist_hosts);

  cass_cluster_set_connection_heartbeat_interval(cluster, 0);

  cass_cluster_set_num_threads_io(cluster, 2);

//  cass_cluster_set_core_connections_per_host(cluster, 2);


  // printf("Main before connect_future = cass_session_connect\n");
  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  // printf("Main AFTER connect_future = cass_session_connect\n");
  if (cass_future_error_code(connect_future) == CASS_OK) {
    int i;
    // printf("Main connect_future) == CASS_OK\n");
//    int n_thread = 10;
    for (i = 0; i < n_thread; i++){
        // printf("Main CREATE thread ======== id : %d\n", i);
        pthread_create(&tid[i], NULL, read_thread, (void *)session);
    }

    int j;
    for (j = 0; j < n_thread; j++){
        pthread_join(tid[j], NULL);
    }

  } else {
    // printf("Main Handle error\n");
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);



  return 0;
}
