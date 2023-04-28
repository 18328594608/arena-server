# include "me_limit.h"
# include "me_market.h"
# include "me_symbol.h"

static nw_timer timer;
static int flag = 0;
static json_t *list;

static void flush_list(void)
{
    const char *symbol;
    json_t *value;
    json_object_foreach(list, symbol, value) {

        market_t *m = get_market(symbol);
        if (m == NULL)
            continue;

        int size = m->limit_buys->len + m->limit_sells->len;
        if (size == 0)
            continue;

        // bid
        mpd_t *bid = mpd_new(&mpd_ctx);
        mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
        if (mpd_cmp(bid, mpd_zero, &mpd_ctx) <= 0) {
            mpd_del(bid);
            continue;
        }
        // ask
        mpd_t *ask = mpd_new(&mpd_ctx);
        mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);
        if (mpd_cmp(ask, mpd_zero, &mpd_ctx) <= 0) {
            mpd_del(bid);
            mpd_del(ask);
            continue;
        }

        // margin price
        mpd_t *margin_bid_price = mpd_new(&mpd_ctx);
        mpd_t *margin_ask_price = mpd_new(&mpd_ctx);
        mpd_copy(margin_bid_price, mpd_one, &mpd_ctx);
        mpd_copy(margin_ask_price, mpd_one, &mpd_ctx);
        symbol_t *sym = get_symbol(symbol);
        if (sym->margin_calc == MARGIN_CALC_FOREX) {
            if (sym->margin_type == MARGIN_TYPE_AC || sym->margin_type == MARGIN_TYPE_BC) {
                mpd_copy(margin_ask_price, symbol_ask(sym->margin_symbol), &mpd_ctx);
                mpd_copy(margin_bid_price, symbol_bid(sym->margin_symbol), &mpd_ctx);
            }
        } else {
            if (strcmp(sym->name, "HSI") == 0) {
                mpd_copy(margin_ask_price, symbol_ask("USDHKD"), &mpd_ctx);
                mpd_copy(margin_bid_price, symbol_bid("USDHKD"), &mpd_ctx);
            } else if (strcmp(sym->name, "DAX") == 0) {
                mpd_copy(margin_ask_price, symbol_ask("EURUSD"), &mpd_ctx);
                mpd_copy(margin_bid_price, symbol_bid("EURUSD"), &mpd_ctx);
            } else if (strcmp(sym->name, "UK100") == 0) {
                mpd_copy(margin_ask_price, symbol_ask("GBPUSD"), &mpd_ctx);
                mpd_copy(margin_bid_price, symbol_bid("GBPUSD"), &mpd_ctx);
            } else if (strcmp(sym->name, "JP225") == 0) {
                mpd_copy(margin_ask_price, symbol_ask("USDJPY"), &mpd_ctx);
                mpd_copy(margin_bid_price, symbol_bid("USDJPY"), &mpd_ctx);
            }
        }

        if (mpd_cmp(margin_bid_price, mpd_zero, &mpd_ctx) <= 0) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(margin_bid_price);
            mpd_del(margin_ask_price);
            continue;
        }

        // buy limit 
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(m->limit_buys);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;

            if (order->type == MARKET_ORDER_TYPE_LIMIT && mpd_cmp(order->price, ask, &mpd_ctx) < 0)
            {
                break;
            }
            else if (order->type == MARKET_ORDER_TYPE_BREAK && mpd_cmp(order->price, ask, &mpd_ctx) > 0)
            {
                break;
            }

            log_info("## [buy limit] %"PRIu64" %s %"PRIu64" - %s at %s", order->sid, symbol, order->id, mpd_to_sci(order->price, 0),  mpd_to_sci(ask, 0));
            int ret = limit_open(true, m, sym, order, order->sid, ask, order->fee, margin_ask_price, current_timestamp());
            if (ret < 0) {
                log_fatal("limit open fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
        skiplist_release_iterator(iter);

        // sell limit 
        iter = skiplist_get_iterator(m->limit_sells);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (order->type == MARKET_ORDER_TYPE_LIMIT && mpd_cmp(order->price, bid, &mpd_ctx) > 0)
            {
                break;
            }
            else if (order->type == MARKET_ORDER_TYPE_BREAK && mpd_cmp(order->price, bid, &mpd_ctx) < 0)
            {
                break;
            }

            log_info("## [sell limit] %"PRIu64" %s %"PRIu64" - %s at %s", order->sid, symbol, order->id, mpd_to_sci(order->price, 0),  mpd_to_sci(bid, 0));
            int ret = limit_open(true, m, sym, order, order->sid, bid, order->fee, margin_bid_price, current_timestamp());
            if (ret < 0) {
                log_fatal("limit open fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
        skiplist_release_iterator(iter);

        mpd_del(bid);
        mpd_del(ask);
        mpd_del(margin_bid_price);
        mpd_del(margin_ask_price);
    }

    json_object_clear(list);
}

static void on_timer(nw_timer *t, void *privdata)
{
    if (flag == 0) {
        flag = 1;
        flush_list();
        flag = 0;
    }
}

int init_limit(void)
{
    list = json_object();

    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);
    return 0;
}

int append_limit_symbol(const char *symbol)
{
    json_object_set(list, symbol, json_integer(0));
    return 0;
}
