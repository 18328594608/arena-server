# ifndef _ME_LOAD_H_
# define _ME_LOAD_H_

# include <stdint.h>
# include "ut_mysql.h"

int load_orders(MYSQL *conn, const char *table);
int load_markets(MYSQL *conn, const char *table);
int load_balance(MYSQL *conn, const char *table);

int load_operlog(MYSQL *conn, const char *table, uint64_t *start_id);

int load_positions(MYSQL *conn, const char *table);
int load_balance_v2(MYSQL *conn, const char *table);

int load_limits(MYSQL *conn, const char *table);

# endif

