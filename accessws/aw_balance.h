# ifndef _AW_BALANCE_H_
# define _AW_BALANCE_H_

int init_balance(void);

int balance_subscribe(nw_ses *ses);
int balance_unsubscribe(nw_ses *ses);

int balance_on_update(json_t *msg);

# endif

