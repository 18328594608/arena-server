# include "me_tpsl.h"
# include "me_market.h"
# include "me_symbol.h"
# include "me_balance.h"
#include "me_trade.h"

static nw_timer timer;
static int flag = 0;
static json_t *list;

static void order_profit(order_t *order, symbol_t *sym, mpd_t *close_price, mpd_t *profit_price)
{
    // 1.计算价格差
    mpd_t *profit = mpd_new(&mpd_ctx);
    if (order->side == ORDER_SIDE_BUY) {
        mpd_sub(profit, close_price, order->price, &mpd_ctx);
    } else {
        mpd_sub(profit, order->price, close_price, &mpd_ctx);
    }
    mpd_mul(profit, profit, order->lot, &mpd_ctx);

    // 2.折算USD
    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        mpd_mul(profit, profit, sym->contract_size, &mpd_ctx);
        if (sym->profit_type == PROFIT_TYPE_UB) {
            mpd_div(profit, profit, close_price, &mpd_ctx);  // USDJPY
        } else if (sym->profit_type == PROFIT_TYPE_AC) {
            mpd_mul(profit, profit, profit_price, &mpd_ctx); // EURGBP
        } else if (sym->profit_type == PROFIT_TYPE_CB) {
            mpd_div(profit, profit, profit_price, &mpd_ctx); // CADJPY
        }
    } else if (sym->profit_calc == PROFIT_CALC_CFD) {
        mpd_mul(profit, profit, sym->contract_size, &mpd_ctx);
        if (strcmp(sym->name, "HSI") == 0) {
            mpd_div(profit, profit, profit_price, &mpd_ctx); // USDHKD
        } else if (strcmp(sym->name, "DAX") == 0) {
            mpd_mul(profit, profit, profit_price, &mpd_ctx); // EURUSD
        } else if (strcmp(sym->name, "UK100") == 0) {
            mpd_mul(profit, profit, profit_price, &mpd_ctx); // GBPUSD
        } else if (strcmp(sym->name, "JP225") == 0) {
            mpd_div(profit, profit, profit_price, &mpd_ctx); // USDJPY
        }
    } else {
        mpd_mul(profit, profit, sym->tick_price, &mpd_ctx);
        mpd_div(profit, profit, sym->tick_size, &mpd_ctx);
    }
    mpd_rescale(profit, profit, -2, &mpd_ctx);

    // 3.更新实时盈亏
    if (mpd_cmp(order->profit, mpd_zero, &mpd_ctx) != 0) {
        balance_sub_float(order->sid, BALANCE_TYPE_FLOAT, order->profit);
    }
    balance_add_float(order->sid, BALANCE_TYPE_FLOAT, profit);

    // 4.更新订单
    mpd_copy(order->profit, profit, &mpd_ctx);
    mpd_copy(order->close_price, close_price, &mpd_ctx);
    mpd_copy(order->profit_price, profit_price, &mpd_ctx);

    mpd_del(profit);
}

static void flush_list(void)
{
    const char *symbol;
    json_t *value;
    json_object_foreach(list, symbol, value) {

//    for (int i = 0; i < configs.symbol_num; ++i) {
//        const char* symbol = configs.symbols[i].name;

        market_t *m = get_market(symbol);
        if (m == NULL)
            continue;

        int size = m->tp_buys->len + m->tp_sells->len + m->sl_buys->len + m->sl_sells->len;
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

        // profit price
        mpd_t *profit_bid_price = mpd_new(&mpd_ctx);
        mpd_t *profit_ask_price = mpd_new(&mpd_ctx);
        mpd_copy(profit_bid_price, mpd_one, &mpd_ctx);
        mpd_copy(profit_ask_price, mpd_one, &mpd_ctx);

        symbol_t *sym = get_symbol(symbol);
        if (sym->profit_calc == PROFIT_CALC_FOREX) {
            if (sym->profit_type == PROFIT_TYPE_AC || sym->profit_type == PROFIT_TYPE_CB) {
                mpd_copy(profit_bid_price, symbol_bid(sym->profit_symbol), &mpd_ctx);
                mpd_copy(profit_ask_price, symbol_ask(sym->profit_symbol), &mpd_ctx);
            }
        } else {
            if (strcmp(sym->name, "HSI") == 0) {
                mpd_copy(profit_bid_price, symbol_bid("USDHKD"), &mpd_ctx);
                mpd_copy(profit_ask_price, symbol_ask("USDHKD"), &mpd_ctx);
            } else if (strcmp(sym->name, "DAX") == 0) {
                mpd_copy(profit_bid_price, symbol_bid("EURUSD"), &mpd_ctx);
                mpd_copy(profit_ask_price, symbol_ask("EURUSD"), &mpd_ctx);
            } else if (strcmp(sym->name, "UK100") == 0) {
                mpd_copy(profit_bid_price, symbol_bid("GBPUSD"), &mpd_ctx);
                mpd_copy(profit_ask_price, symbol_ask("GBPUSD"), &mpd_ctx);
            } else if (strcmp(sym->name, "JP225") == 0) {
                mpd_copy(profit_bid_price, symbol_bid("USDJPY"), &mpd_ctx);
                mpd_copy(profit_ask_price, symbol_ask("USDJPY"), &mpd_ctx);
            }
        }

        if (mpd_cmp(profit_bid_price, mpd_zero, &mpd_ctx) <= 0) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(profit_bid_price);
            mpd_del(profit_ask_price);
            continue;
        }

        uint64_t *order_ids = (uint64_t *) malloc(size * sizeof(uint64_t));
        int total = 0;

        // buy [tp] close by bid
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(m->tp_buys);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(bid, order->tp, &mpd_ctx) < 0)
                break;

            log_info("## [tp] %"PRIu64" buy %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                    order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(bid, 0));
            order_profit(order, sym, bid, profit_bid_price);
            order->comment = strdup("tp");
            order->finish_time = current_timestamp();
            order_ids[total] = order->id;
            total++;
        }
        skiplist_release_iterator(iter);

        // buy [sl] close by bid
        iter = skiplist_get_iterator(m->sl_buys);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(bid, order->sl, &mpd_ctx) > 0)
                break;

            log_info("## [sl] %"PRIu64" buy %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                    order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(bid, 0));
            order_profit(order, sym, bid, profit_bid_price);
            order->comment = strdup("sl");
            order->finish_time = current_timestamp();
            order_ids[total] = order->id;
            total++;
        }
        skiplist_release_iterator(iter);

        // sell [tp] close by ask
        iter = skiplist_get_iterator(m->tp_sells);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(ask, order->tp, &mpd_ctx) > 0)
                break;

            log_info("## [tp] %"PRIu64" sell %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                    order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(ask, 0));
            order_profit(order, sym, ask, profit_ask_price);
            order->comment = strdup("tp");
            order->finish_time = current_timestamp();
            order_ids[total] = order->id;
            total++;
        }
        skiplist_release_iterator(iter);

        // sell [sl] close by ask
        iter = skiplist_get_iterator(m->sl_sells);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            if (mpd_cmp(ask, order->sl, &mpd_ctx) < 0)
                break;

            log_info("## [sl] %"PRIu64" sell %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                    order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(ask, 0));
            order_profit(order, sym, ask, profit_ask_price);
            order->comment = strdup("sl");
            order->finish_time = current_timestamp();
            order_ids[total] = order->id;
            total++;
        }
        skiplist_release_iterator(iter);

/*
        // buy orders
        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(m->tpsl_buys);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;

            // [tp] close by bid
            if (mpd_cmp(order->tp, mpd_zero, &mpd_ctx) > 0 && mpd_cmp(bid, order->tp, &mpd_ctx) >= 0) {
                log_info("## [tp] %"PRIu64" buy %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                         order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(bid, 0));
                order_profit(order, sym, bid, profit_bid_price);
                order->comment = strdup("tp");
                order->finish_time = current_timestamp();
                order_ids[total] = order->id;
                total++;
                continue;
            } 

            // [sl] close by bid
            if (mpd_cmp(order->sl, mpd_zero, &mpd_ctx) > 0 && mpd_cmp(bid, order->sl, &mpd_ctx) <= 0) {
                log_info("## [sl] %"PRIu64" buy %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                         order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(bid, 0));
                order_profit(order, sym, bid, profit_bid_price);
                order->comment = strdup("sl");
                order->finish_time = current_timestamp();
                order_ids[total] = order->id;
                total++;
            } 
        }
        skiplist_release_iterator(iter);

        // sell orders
        iter = skiplist_get_iterator(m->tpsl_sells);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;

            // [tp] close by ask
            if (mpd_cmp(order->tp, mpd_zero, &mpd_ctx) > 0 && mpd_cmp(ask, order->tp, &mpd_ctx) <= 0) {
                log_info("## [tp] %"PRIu64" sell %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                         order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(ask, 0));
                order_profit(order, sym, ask, profit_ask_price);
                order->comment = strdup("tp");
                order->finish_time = current_timestamp();
                order_ids[total] = order->id;
                total++;
                continue;
            } 

            // [sl] close by ask
            if (mpd_cmp(order->sl, mpd_zero, &mpd_ctx) > 0 && mpd_cmp(ask, order->sl, &mpd_ctx) >= 0) {
                log_info("## [sl] %"PRIu64" sell %s [%"PRIu64"] [%s / %s] at %s", order->sid, symbol,
                         order->id, mpd_to_sci(order->tp, 0), mpd_to_sci(order->sl, 0), mpd_to_sci(ask, 0));
                order_profit(order, sym, ask, profit_ask_price);
                order->comment = strdup("sl");
                order->finish_time = current_timestamp();
                order_ids[total] = order->id;
                total++;
            } 
        }
        skiplist_release_iterator(iter);
*/

        mpd_del(bid);
        mpd_del(ask);
        mpd_del(profit_bid_price);
        mpd_del(profit_ask_price);

        // close order
        int ret = 0;
        for (int j = 0; j < total; ++j) {
            ret = market_tpsl_hedged(m, order_ids[j]);
            if (ret < 0)
                log_fatal("tpsl fail: %d, order: %"PRIu64"", ret, order_ids[j]);
        }
        free(order_ids);
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

int init_tpsl(void)
{
    list = json_object();

    nw_timer_set(&timer, 0.2, true, on_timer, NULL);
    nw_timer_start(&timer);
    return 0;
}

int append_tpsl(const char *symbol)
{
    json_object_set(list, symbol, json_integer(0));
    return 0;
}
