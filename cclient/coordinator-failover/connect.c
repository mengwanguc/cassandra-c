#include <cassandra.h>
/* Use "#include <dse.h>" when connecting to DataStax Enterpise */
#include <stdio.h>

void handle_query_result(CassFuture* future);

void execute_query(CassSession* session) {
  /* Create a statement with zero parameters */
  CassStatement* statement
    = cass_statement_new("select name from mittcpu.students where id = 1", 0);

  CassFuture* query_future = cass_session_execute(session, statement);

  /* Statement objects can be freed immediately after being executed */
  cass_statement_free(statement);

  /* This will block until the query has finished */
  CassError rc = cass_future_error_code(query_future);

  printf("Query result: %s\n", cass_error_desc(rc));

  handle_query_result(query_future);

}

void handle_query_result(CassFuture* future) {
  /* This will also block until the query returns */
  const CassResult* result = cass_future_get_result(future);


  /* If there was an error then the result won't be available */
  if (result == NULL) {

    /* Handle error */

    cass_future_free(future);
    return;
  }

  /* The future can be freed immediately after getting the result object */
  cass_future_free(future);

  printf("after free\n");

  /* This can be used to retrieve the first row of the result */
  const CassRow* row = cass_result_first_row(result);

  printf("after get row\n");

  /* Now we can retrieve the column values from the row */
  const char* name;
  size_t name_length;
  /* Get the column value of "key" by name */
  cass_value_get_string(cass_row_get_column_by_name(row, "name"), &name, &name_length);

  printf("after get string\n");
  /* This will free the result as well as the string pointed to by 'key' */
  cass_result_free(result);
}


int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "heavy-client.cass-5n.ucare.emulab.net");

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  /* This operation will block until the result is ready */
  CassError rc = cass_future_error_code(connect_future);

  if (rc != CASS_OK) {
    /* Display connection error message */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Connect error: '%.*s'\n", (int)message_length, message);
  }

  /* Run queries... */
  execute_query(session);


  cass_future_free(connect_future);
  cass_session_free(session);
  cass_cluster_free(cluster);

  return 0;
}
