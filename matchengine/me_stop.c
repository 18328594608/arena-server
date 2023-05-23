# include "me_stop.h"
# include "me_tick.h"
# include "me_market.h"
# include "me_symbol.h"
# include "me_balance.h"
#include "me_trade.h"
static nw_timer timer;
static int flag = 0;
static json_t *list;
static int pos = 0; // 当前品种索引
static int cap = 1; // 每次处理有效品种的数量
static int start = 0; // 收到tick再开始风控

static void float_profit(order_t *order, symbol_t *sym, mpd_t *close_price, mpd_t *profit_price)
{
    uint64_t sid = order->sid;
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

    // 3.撤销上次盈亏
    if (mpd_cmp(order->profit, mpd_zero, &mpd_ctx) != 0) {
        balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);
    }

    // 4.更新这次盈亏
    mpd_copy(order->profit, profit, &mpd_ctx);
    mpd_copy(order->close_price, close_price, &mpd_ctx);
    mpd_copy(order->profit_price, profit_price, &mpd_ctx);

    balance_add_float(sid, BALANCE_TYPE_FLOAT, profit);
    mpd_del(profit);
}

static void margin_stop_out(uint64_t sid)
{ 
    while (true) {
        mpd_t *margin = balance_get_v2(sid, BALANCE_TYPE_MARGIN);
        // 无持仓单
        if (margin == NULL || mpd_cmp(margin, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        mpd_t *equity = balance_get_v2(sid, BALANCE_TYPE_EQUITY);
        mpd_t *pnl = balance_get_v2(sid, BALANCE_TYPE_FLOAT);
        mpd_t *ml = mpd_new(&mpd_ctx);
        mpd_add(ml, equity, pnl, &mpd_ctx);
        char *temp = mpd_to_sci(ml, 0);
        mpd_div(ml, ml, margin, &mpd_ctx);

        if (mpd_cmp(ml, settings.stop_out, &mpd_ctx) >= 0) {
            mpd_del(ml);
            free(temp);
            break;
        }

        log_info("## [%"PRIu64"] equity = %s, pnl = %s, margin = %s", sid, mpd_to_sci(equity, 0), mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0));

        mpd_rescale(ml, ml, -3, &mpd_ctx);
        sds comment = sdsempty();
        comment = sdscatprintf(comment, "so:%s/%s/%s", mpd_to_sci(ml, 0), temp, mpd_to_sci(margin, 0));
        log_info("## [%"PRIu64"] stop out comment = %s", sid, comment);

        mpd_t *profit = mpd_new(&mpd_ctx);
        mpd_copy(profit, mpd_zero, &mpd_ctx);
        uint64_t id = 0;
        char *so_symbol = "";

        for (int i = 0; i < configs.symbol_num; ++i) {
            const char* symbol = configs.symbols[i].name;
            if (strcmp(symbol, "USDHKD") == 0)
                continue;

            market_t *m = get_market(symbol);
            if (m == NULL)
                continue;

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list == NULL)
                continue;

            skiplist_node *node;
            skiplist_iter *iter = skiplist_get_iterator(list);
            while ((node = skiplist_next(iter)) != NULL) {
                order_t *order = node->value;
                if (id == 0 || mpd_cmp(profit, order->profit, &mpd_ctx) > 0) {
                    // 如果平仓价格为0，说明还没收到过行情，跳过这笔订单
                    if (mpd_cmp(order->close_price, mpd_zero, &mpd_ctx) > 0) {
                        mpd_copy(profit, order->profit, &mpd_ctx);
                        id = order->id;
                        so_symbol = strdup(order->symbol);
                    }
                }
            }
            skiplist_release_iterator(iter);
        }

        log_info("## [%"PRIu64"] stop out symbol = %s, id = %"PRIu64"", sid, so_symbol, id);
        if (id == 0) {
            mpd_del(profit);
            free(temp);
            sdsfree(comment);
            break;
        }

        market_t *m = get_market(so_symbol);
        order_t *order = market_get_order(m, id);
        double finish_time = current_timestamp();
        int ret = market_stop_out_hedged(m, sid, order, comment, finish_time);
        if (ret < 0)
            log_error("market_stop_out fail: %"PRIu64"", order->id);
    
        mpd_del(profit);
        free(temp);
        free(so_symbol);
        sdsfree(comment);
    }
}

static void flush_list(void)
{
//    for (int i = 0; i < configs.symbol_num; ++i) {
//        const char* symbol = configs.symbols[i].name;
//        if (strcmp(symbol, "USDHKD") == 0)
//            continue;

    int count = 0;
    int from = pos;
    while (count < cap) {
        pos++;
        if (pos > configs.symbol_num - 1)
            pos = 0;

        // 防止死循环，遍历所有品种没有符合条件的
        if (from == pos) {
//log_info("## break ##");
            break;
        }

        const char* symbol = configs.symbols[pos].name;
//log_info("## [%d].symbol = %s ##", pos, symbol);
        if (json_object_get(list, symbol) == NULL) {
//log_info("## continue-11 ##");
            continue;
	}

        // bid
        mpd_t *bid = mpd_new(&mpd_ctx);
        mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
        if (mpd_cmp(bid, mpd_zero, &mpd_ctx) <= 0) {
            mpd_del(bid);
//log_info("## continue-22 ##");
            continue;
        }

        // ask
        mpd_t *ask = mpd_new(&mpd_ctx);
        mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);

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

        json_object_del(list, symbol);
        count++;
        // TODO 每次最多处理 10000 个帐号的风控
        uint64_t *sids = (uint64_t *) malloc(10000 * sizeof(uint64_t));
        int total = 0;
        uint64_t sid = 0;

        market_t *m = get_market(symbol);
        dict_iterator *iter = dict_get_iterator(m->users);
        dict_entry *entry;

        while ((entry = dict_next(iter)) != NULL) {
            // 1.更新浮动盈亏
            skiplist_node *node;
            skiplist_iter *it = skiplist_get_iterator(entry->val);
            while ((node = skiplist_next(it)) != NULL) {
                order_t *order = node->value;
                if (sid == 0)
                    sid = order->sid;

                if (order->side == ORDER_SIDE_BUY) {
                    float_profit(order, sym, bid, profit_bid_price);
                } else {
                    float_profit(order, sym, ask, profit_ask_price);
                }
            }
            skiplist_release_iterator(it);

            // 2.判断 margin level
            mpd_t *balance = balance_get_v2(sid, BALANCE_TYPE_BALANCE);
            mpd_t *pnl = balance_get_v2(sid, BALANCE_TYPE_FLOAT);
            if (pnl == NULL)
                continue;

            if (mpd_cmp(balance, mpd_zero, &mpd_ctx) > 0 && mpd_cmp(pnl, mpd_zero, &mpd_ctx) >= 0) {
                sid = 0;
                continue;
            }

            mpd_t *ml = mpd_new(&mpd_ctx);
            mpd_t *equity = balance_get_v2(sid, BALANCE_TYPE_EQUITY);
            mpd_add(ml, equity, pnl, &mpd_ctx);
            mpd_t *margin = balance_get_v2(sid, BALANCE_TYPE_MARGIN);
            mpd_div(ml, ml, margin, &mpd_ctx);

            if (mpd_cmp(ml, settings.stop_out, &mpd_ctx) < 0) {
                // 3.符合条件，稍后处理 
                sids[total] = sid;
                total++;

                if (total > 9999) {
                    log_error("[stop out] total > 9999");
                    break;
                }
            }
            mpd_del(ml);
            sid = 0;
        }
        dict_release_iterator(iter);

        mpd_del(bid);
        mpd_del(ask);
        mpd_del(profit_bid_price);
        mpd_del(profit_ask_price);

        // 4.stop out
        for (int j = 0; j < total; ++j) {
             margin_stop_out(sids[j]);
        }
        free(sids);
    }
}

static void on_timer(nw_timer *t, void *privdata)
{
    if (start > flag) {
        flag = 1;
        flush_list();
        flag = 0;
    }
}

int init_stop_out(void)
{
    list = json_object();

    nw_timer_set(&timer, 0.1, true, on_timer, NULL);
    nw_timer_start(&timer);
    return 0;
}

int append_stop_symbol(const char *symbol)
{
    start = 1;
    json_object_set(list, symbol, json_integer(0));
    return 0;
}
