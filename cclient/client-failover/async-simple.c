#include <cassandra.h>
#include <unistd.h>
#include <stdio.h>	/* for printf */
#include <stdint.h>	/* for uint64 definition */
#include <stdlib.h>	/* for exit() definition */
#include <time.h>	/* for clock_gettime */

#define BILLION 1000000000L

void on_result(CassFuture* result_future, void* data) {
    struct timespec start = *(struct timespec*)data;
    struct timespec end;
    uint64_t diff;

    clock_gettime(CLOCK_MONOTONIC, &end);

    diff = BILLION * (end.tv_sec - start.tv_sec) + end.tv_nsec - start.tv_nsec;
    printf("elapsed time = %llu nanoseconds\n", (long long unsigned int) diff);


    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);

      if (row) {
        const CassValue* value = cass_row_get_column_by_name(row, "name");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("name: '%.*s'\n", (int)release_version_length, release_version);


      }

      cass_result_free(result);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }

}


int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "155.98.38.221";
//  char* hosts = "155.98.38.109";
  char* whilelist_hosts = "155.98.38.221";

  if (argc > 1) {
    hosts = argv[1];
  }

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  cass_cluster_set_whitelist_filtering(cluster, whilelist_hosts);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
//    const char* query = "SELECT release_version FROM system.local";
    int i = 0;
    sleep(5);
    for (i = 0; i < 10; i++) {
      char query[100];
      snprintf(query, 100, "SELECT name FROM mittcpu.students WHERE id = %d", i+1);
      CassStatement* statement = cass_statement_new(query, 0);
      struct timespec start;
      clock_gettime(CLOCK_MONOTONIC, &start);


      CassFuture* result_future = cass_session_execute(session, statement);

      cass_future_set_callback(result_future, on_result, &start);

      cass_statement_free(statement);
      cass_future_free(result_future);
      usleep(10000);
    }
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

  return 0;
}
