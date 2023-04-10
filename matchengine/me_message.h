# ifndef _ME_MESSAGE_H_
# define _ME_MESSAGE_H_

# include "me_config.h"
# include "me_market.h"

int init_message(void);
int fini_message(void);

enum {
    ORDER_EVENT_OPEN    = 1,
    ORDER_EVENT_UPDATE  = 2,
    ORDER_EVENT_CLOSE   = 3,
    ORDER_EVENT_TPSL    = 4,
    ORDER_EVENT_STOP    = 5,
    ORDER_EVENT_LIMIT   = 6,
    ORDER_EVENT_CANCEL  = 7,
    ORDER_EVENT_EXPIRE  = 8,

    ORDER_EVENT_PUT     = 11,
    ORDER_EVENT_FINISH  = 13,
};

int push_balance_message(double t, uint32_t user_id, const char *asset, const char *business, mpd_t *change);
int push_order_message(uint32_t event, order_t *order, market_t *market);
int push_deal_message(double t, const char *market, order_t *ask, order_t *bid, mpd_t *price, mpd_t *amount,
        mpd_t *ask_fee, mpd_t *bid_fee, int side, uint64_t id, const char *stock, const char *money);

int push_balance_message_v2(double t, uint64_t sid, mpd_t *change, mpd_t *balance, const char *comment);
int push_order_message_v2(uint32_t event, order_t *order);

bool is_message_block(void);
sds message_status(sds reply);

# endif

