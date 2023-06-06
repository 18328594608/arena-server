# ifndef _ME_MARKET_H_
# define _ME_MARKET_H_

# include "me_config.h"
# include "me_symbol.h"

extern uint64_t order_id_start;
extern uint64_t deals_id_start;
extern skiplist_t *expire_orders;

typedef struct order_t {
    uint64_t        id;
    uint64_t        external;
    uint32_t        type;
    uint32_t        side;
    double          create_time;
    double          update_time;
    double          finish_time;
    uint64_t        expire_time;
    uint64_t        sid;
    char            *symbol;
    char            *comment;
    mpd_t           *lot;
    mpd_t           *price;
    mpd_t           *close_price;
    mpd_t           *margin;
    mpd_t           *fee;
    mpd_t           *swap;
    mpd_t           *swaps;
    mpd_t           *profit;
    mpd_t           *tp;
    mpd_t           *sl;
    mpd_t           *margin_price;
    mpd_t           *profit_price;

    uint32_t        user_id;
} order_t;

typedef struct market_t {
    char            *name;

    dict_t          *orders;
    dict_t          *users;
    dict_t          *margins;

    skiplist_t      *buys;
    skiplist_t      *sells;

    dict_t          *limit_orders;
    dict_t          *limit_users;
    skiplist_t      *limit_buys;
    skiplist_t      *limit_sells;

    skiplist_t      *asks;
    skiplist_t      *bids;

    skiplist_t      *tp_buys;
    skiplist_t      *sl_buys;
    skiplist_t      *tp_sells;
    skiplist_t      *sl_sells;
} market_t;

//market_t *market_create(struct market *conf);
int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, size_t *bid_count, mpd_t *bid_amount);

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source);
int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *taker_fee, const char *source);
int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order);

int market_put_order(market_t *m, order_t *order);

json_t *get_order_info(order_t *order);
order_t *market_get_order(market_t *m, uint64_t id);
skiplist_t *market_get_order_list(market_t *m, uint32_t user_id);

sds market_status(sds reply);

market_t *market_create_v2(struct symbol *conf);

int market_open(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot,
                mpd_t *tp, mpd_t *sl, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, mpd_t *margin_price, double create_time);
int market_put_position(market_t *m, order_t *order);
int market_close(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time);
int market_tpsl(bool real, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time);
int market_update(bool real, json_t **result, market_t *m, order_t *order, mpd_t *tp, mpd_t *sl);
int change_order_external(bool real, json_t **result, market_t *m, order_t *order, uint64_t external);
int market_stop_out(bool real, market_t *m, uint64_t sid, order_t *order, const char *comment, double finish_time);

int market_open_hedged(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot,
                mpd_t *tp, mpd_t *sl, mpd_t *percentage, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, mpd_t *margin_price, double create_time);
int market_close_hedged(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time);
int market_tpsl_hedged(market_t *m, uint64_t order_id);
int market_stop_out_hedged(market_t *m, uint64_t sid, order_t *order, const char *comment, double finish_time);

// limit
int market_put_limit(bool real, json_t **result, market_t *m, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot,
                mpd_t *tp, mpd_t *sl, mpd_t *percentage, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, double create_time, uint64_t expire_time, uint32_t type);
order_t *market_get_limit(market_t *m, uint64_t id);
int market_cancel(bool real, json_t **result, market_t *m, order_t *order, const char *comment, double finish_time);
int market_put_pending(market_t *m, order_t *order);
int limit_open(bool real, market_t *m, symbol_t *sym, order_t *order, uint64_t sid, mpd_t *price, mpd_t *fee, mpd_t *margin_price, double update_time);
int limit_expire(order_t *order);

skiplist_t *market_get_order_list_v2(market_t *m, uint64_t sid);
json_t *get_order_info_v2(order_t *order);
order_t *market_get_external_order(market_t *m, uint64_t sid, uint64_t external);
order_t *market_get_external_limit(market_t *m, uint64_t sid, uint64_t external);

# endif

