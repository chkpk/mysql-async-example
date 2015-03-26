/* 
  you need masriadb-connector-c and libevent to compile this example:
 
  gcc -std=gnu11 -o async mysql_async_example.c -lmariadb -levent

 */ 

#include <string.h>
#include <mysql.h>
#include <my_global.h>
#include <my_sys.h>
#include <event.h>

static int opt_connections= 10;
static int opt_query_num = 10000;
static const char *opt_db= "test";
static const char *opt_user= "root";
static const char *opt_password= "";
static const char *opt_host= "127.0.0.1";
static const char *my_groups[]= { "client", NULL };

enum State{
  CONNECT_START,
  CONNECT_WAITING,
  CONNECT_DONE,

  QUERY_START,
  QUERY_WAITING,
  QUERY_RESULT_READY,

  FETCH_ROW_START,
  FETCH_ROW_WAITING,
  FETCH_ROW_RESULT_READY,

  CLOSE_START,
  CLOSE_WAITING,
  CLOSE_DONE
};

// Maintaining a list of queries to run. 
struct query_entry {
  struct query_entry *next;
  char *query;
  int index;
};
static struct query_entry *query_list;  // query task list
static struct query_entry **tail_ptr= &query_list;

static struct timeval t1,t2;
static bool finished = 0;

static int active_connection_num = 0;
static int query_counter= 0;

// State kept for each connection. 
struct mysql_connection {
  int current_state;                   // State machine current state
  MYSQL mysql;                        
  MYSQL *ret;
  MYSQL_RES *result;
  MYSQL_ROW row;
  struct event mysql_event;           // for libevent
  struct query_entry *current_query_entry;
  int err;
  int index;
};

static void fatal(struct mysql_connection *conn, const char *msg) {
  fprintf(stderr, "%s: %s\n", msg, (conn ? mysql_error(&conn->mysql) : ""));
  exit(1);
}

// libevent callback
static void state_machine_handler(int fd, short event, void *arg);

// add event to libevent
static inline int add_event(int status, struct mysql_connection *conn) {
  short wait_event= 0;
  if (status & MYSQL_WAIT_READ)
    wait_event|= EV_READ;
  if (status & MYSQL_WAIT_WRITE)
    wait_event|= EV_WRITE;

  int fd = -1;
  if (wait_event) fd= mysql_get_socket(&conn->mysql);

  struct timeval tv, *ptv = NULL;
  if (status & MYSQL_WAIT_TIMEOUT) {
    tv.tv_sec= mysql_get_timeout_value(&conn->mysql);
    tv.tv_usec= 0;
    ptv= &tv;
  } 

  event_set(&conn->mysql_event, fd, wait_event, state_machine_handler, conn);
  event_add(&conn->mysql_event, ptv);
  return 0;
}

// op_status  : return value of a db operation
// state_wait : one of ( CONNECT_WAITING, QUERY_WAITING, FETCH_ROW_WAITING, CLOSE_WAITING)
static inline int decide_next_state(int op_status, struct mysql_connection* conn, 
    int state_wait, int state_go_on){ 
  if (op_status){  
    // need to wait for data(add event to libevent)
    conn->current_state = state_wait;
    add_event(op_status, conn);   
    return 0;
  } else {        
    // no need to wait, go to next state immediately
    conn->current_state = state_go_on;
    return 1;
  }
}

// event : libevent -> mysql
static int mysql_status(short event) {
  int status= 0;
  if (event & EV_READ)
    status|= MYSQL_WAIT_READ;
  if (event & EV_WRITE)
    status|= MYSQL_WAIT_WRITE;
  if (event & EV_TIMEOUT)
    status|= MYSQL_WAIT_TIMEOUT;
  return status;
}

// call this to entry state_machine
static void state_machine_handler(int fd , short event, void *arg) {
  struct mysql_connection *conn= (struct mysql_connection*)arg;
  int status;
  int state_machine_continue = 1;

  while ( state_machine_continue){
    switch(conn->current_state) {

      case CONNECT_START:   
        status= mysql_real_connect_start(&conn->ret, &conn->mysql, opt_host, opt_user, opt_password, opt_db, 0, NULL, 0);
        state_machine_continue = decide_next_state(status, conn, CONNECT_WAITING, CONNECT_DONE);   
        break;

      case CONNECT_WAITING: 
        status= mysql_real_connect_cont(&conn->ret, &conn->mysql, mysql_status(event));
        state_machine_continue = decide_next_state(status, conn, CONNECT_WAITING, CONNECT_DONE);   
        break;

      case CONNECT_DONE:  
        if (!conn->ret)  fatal(conn, "Failed to mysql_real_connect()");
        conn->current_state = QUERY_START; 
        break;

      case QUERY_START: 
        conn->current_query_entry= query_list;
        if (!conn->current_query_entry) {  
          conn->current_state = CLOSE_START;  
          break;
        }
        query_list= query_list->next;

        if (conn->current_query_entry->index == 0)
          gettimeofday(&t1,NULL);  // start time

        status= mysql_real_query_start(&conn->err, &conn->mysql, conn->current_query_entry->query,
            strlen(conn->current_query_entry->query));
        state_machine_continue = decide_next_state(status, conn, QUERY_WAITING, QUERY_RESULT_READY);   
        break;

      case QUERY_WAITING:
        status= mysql_real_query_cont(&conn->err, &conn->mysql, mysql_status(event));
        state_machine_continue = decide_next_state(status, conn, QUERY_WAITING, QUERY_RESULT_READY);   
        break;

      case QUERY_RESULT_READY:
        if (conn->err) {
          printf("%d | Error: %s\n", conn->index, mysql_error(&conn->mysql));
          conn->current_state = CONNECT_DONE;
          break;
        }
        if (conn->current_query_entry->index == query_counter - 1)
          gettimeofday(&t2,NULL);  // finish time
        free(conn->current_query_entry->query);
        free(conn->current_query_entry);
        conn->result= mysql_use_result(&conn->mysql);
        conn->current_state = (conn->result)? FETCH_ROW_START : QUERY_START;
        break;

      case FETCH_ROW_START:
        status= mysql_fetch_row_start(&conn->row, conn->result);
        state_machine_continue = decide_next_state(status, conn, 
            FETCH_ROW_WAITING, FETCH_ROW_RESULT_READY);   
        break;

      case FETCH_ROW_WAITING:
        status= mysql_fetch_row_cont(&conn->row, conn->result, mysql_status(event));
        state_machine_continue = decide_next_state(status, conn, FETCH_ROW_WAITING, FETCH_ROW_RESULT_READY);   
        break;

      case FETCH_ROW_RESULT_READY: 
        if (!conn->row){
          if (mysql_errno(&conn->mysql)) {
            printf("%d | Error: %s\n", conn->index, mysql_error(&conn->mysql));
          } else {
            /* EOF.no more rows */
          }
          mysql_free_result(conn->result);
          conn->current_state = QUERY_START; 
          break;
        }
        // print fields in the row
        /*
        printf("%d : %d  - ", conn->index, conn->current_query_entry->index);
        for (int i= 0; i < mysql_num_fields(conn->result); i++)
          printf("%s%s", (i ? "\t" : ""), (conn->row[i] ? conn->row[i] : "(null)"));
        printf ("\n");
        */
        conn->current_state = FETCH_ROW_START;
        break;

      case CLOSE_START:
        status= mysql_close_start(&conn->mysql);
        state_machine_continue = decide_next_state(status, conn, CLOSE_WAITING, CLOSE_DONE);   
        break;

      case CLOSE_WAITING: 
        status= mysql_close_cont(&conn->mysql, mysql_status(event));
        state_machine_continue = decide_next_state(status, conn, CLOSE_WAITING, CLOSE_DONE);   
        break;

      case CLOSE_DONE:
        active_connection_num--;
        if (active_connection_num == 0){
          event_loopbreak();
        }
        state_machine_continue = 0;   
        break;

      default:
        abort();
    }
  }
}


// add query to query task list
void add_query(const char *q) {
  struct query_entry *entry = (struct query_entry*)malloc(sizeof(struct query_entry));
  if (!entry) fatal(NULL, "Out of memory");
  entry->query = (char*)malloc(strlen(q) + 1);
  if (!entry->query) fatal(NULL, "Out of memory");

  strcpy(entry->query, q);
  entry->next= NULL;
  entry->index= query_counter++;
  *tail_ptr= entry;
  tail_ptr= &entry->next;
}


int main(int argc, char *argv[]) {
  opt_query_num = (argc > 1)? atoi(argv[1]) : opt_query_num;
  opt_connections = (argc > 2)? atoi(argv[2]) : opt_connections;
  opt_db = (argc > 3)? argv[3] : opt_db;
  opt_user = (argc > 4)? argv[4] : opt_user;
  opt_password = (argc > 5)? argv[5] : opt_password;
  opt_host = (argc > 6)? argv[6] : opt_host;

  for (int i = 0; i < opt_query_num; i++)
    add_query("insert into z1 values(1)");

  struct mysql_connection *conns;
  conns= (struct mysql_connection*)calloc(opt_connections, sizeof(struct mysql_connection));
  if (!conns) fatal(NULL, "Out of memory");

  struct event_base *libevent_base = event_init();
  int err = mysql_library_init(argc, argv, (char **)my_groups);
  if (err) {
    fprintf(stderr, "Fatal: mysql_library_init() returns error: %d\n", err);
    exit(1);
  }

  for (int i= 0; i < opt_connections; i++) {
    mysql_init(&conns[i].mysql);
    mysql_options(&conns[i].mysql, MYSQL_OPT_NONBLOCK, NULL);
    mysql_options(&conns[i].mysql, MYSQL_READ_DEFAULT_GROUP, "async_queries");

    // set timeouts to 300 microseconds 
    uint default_timeout = 300;
    mysql_options(&conns[i].mysql, MYSQL_OPT_READ_TIMEOUT, &default_timeout);
    mysql_options(&conns[i].mysql, MYSQL_OPT_CONNECT_TIMEOUT, &default_timeout);
    mysql_options(&conns[i].mysql, MYSQL_OPT_WRITE_TIMEOUT, &default_timeout);

    conns[i].current_state= CONNECT_START;
    conns[i].index = i;
    active_connection_num++;
    state_machine_handler(-1, -1, &conns[i]);
  }

  event_dispatch();

  long diff = (t2.tv_sec - t1.tv_sec)*1000000 + t2.tv_usec - t1.tv_usec;
  printf("diff: %d\n", diff); 

  free(conns);
  mysql_library_end();
  event_base_free(libevent_base);

  return 0;
}
