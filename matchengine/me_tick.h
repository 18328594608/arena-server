# ifndef _ME_TICK_H_
# define _ME_TICK_H_

# include "ut_decimal.h"
# include "uwsc.h"

int init_tick(void);
mpd_t* symbol_bid(const char *symbol);
mpd_t* symbol_ask(const char *symbol);

void reconnect(struct uwsc_client *cl);

int tick_status();

# endif
