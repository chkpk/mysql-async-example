/* 
  you need mysql-connector-c to compile this example:
 
  gcc -std=gnu11 -o sync mysql_example.c -lmysqlclient 

 */ 

#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <mysql.h>

static int opt_connections= 10;
static int opt_query_num = 10000;
static const char *opt_db= "test";
static const char *opt_user= "root";
static const char *opt_password= "";
static const char *opt_host= "127.0.0.1";
static const char *my_groups[]= { "client", NULL };

struct query_entry {
  struct query_entry *next;
  char *query;
  int index;
};
static struct query_entry *query_list;
static struct query_entry **tail_ptr= &query_list;

static int active_connection_num = 0;
static int query_counter= 0;
struct timeval t1,t2;

static void fatal(MYSQL *conn, const char *msg) {
  fprintf(stderr, "%s: %s\n", msg, (conn ? mysql_error(conn) : ""));
  exit(1);
}

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

  MYSQL* mysql = mysql_init(NULL);
  if (mysql_real_connect(mysql, opt_host, opt_user, opt_password, opt_db, 0, NULL, 0) == NULL)
    fatal(mysql, "connect error");

  struct query_entry* current_query;

  gettimeofday(&t1,NULL);
  for (int i = 0; i < opt_query_num; i ++){
    current_query = query_list;
    query_list= query_list->next;
    if (mysql_query(mysql,current_query->query))
      fprintf(stderr,"query error : %s\n", mysql_error(mysql));
    free(current_query->query);
    free(current_query);
  }

  gettimeofday(&t2,NULL);
  long diff = (t2.tv_sec - t1.tv_sec)*1000000 + t2.tv_usec - t1.tv_usec;
  printf("diff: %d\n", diff); 

  mysql_close(mysql);
  mysql_library_end();
  return 0;
}
