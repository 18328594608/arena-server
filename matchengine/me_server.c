# include "me_config.h"
# include "me_server.h"
# include "me_balance.h"
# include "me_update.h"
# include "me_market.h"
# include "me_trade.h"
# include "me_operlog.h"
# include "me_history.h"
# include "me_message.h"
# include "me_symbol.h"
# include "me_tick.h"

static rpc_svr *svr;
static dict_t *dict_cache;
static nw_timer cache_timer;

struct cache_val {
    double      time;
    json_t      *result;
};

static int reply_json(nw_ses *ses, rpc_pkg *pkg, const json_t *json)
{
    char *message_data;
    if (settings.debug) {
        message_data = json_dumps(json, JSON_INDENT(4));
    } else {
        message_data = json_dumps(json, 0);
    }
    if (message_data == NULL)
        return -__LINE__;
    log_trace("connection: %s send: %s", nw_sock_human_addr(&ses->peer_addr), message_data);

    rpc_pkg reply;
    memcpy(&reply, pkg, sizeof(reply));
    reply.pkg_type = RPC_PKG_TYPE_REPLY;
    reply.body = message_data;
    reply.body_size = strlen(message_data);
    rpc_send(ses, &reply);
    free(message_data);

    return 0;
}

static int reply_error(nw_ses *ses, rpc_pkg *pkg, int code, const char *message)
{
    json_t *error = json_object();
    json_object_set_new(error, "code", json_integer(code));
    json_object_set_new(error, "message", json_string(message));

    json_t *reply = json_object();
    json_object_set_new(reply, "error", error);
    json_object_set_new(reply, "result", json_null());
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_error_invalid_argument(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 1, "invalid argument");
}

static int reply_error_internal_error(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 2, "internal error");
}

static int reply_error_service_unavailable(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 3, "service unavailable");
}

static int reply_error_market_close(nw_ses *ses, rpc_pkg *pkg)
{
    return reply_error(ses, pkg, 20, "market is close");
}

static int reply_result(nw_ses *ses, rpc_pkg *pkg, json_t *result)
{
    json_t *reply = json_object();
    json_object_set_new(reply, "error", json_null());
    json_object_set    (reply, "result", result);
    json_object_set_new(reply, "id", json_integer(pkg->req_id));

    int ret = reply_json(ses, pkg, reply);
    json_decref(reply);

    return ret;
}

static int reply_success(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *result = json_object();
    json_object_set_new(result, "status", json_string("success"));

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static bool process_cache(nw_ses *ses, rpc_pkg *pkg, sds *cache_key)
{
    sds key = sdsempty();
    key = sdscatprintf(key, "%u", pkg->command);
    key = sdscatlen(key, pkg->body, pkg->body_size);
    dict_entry *entry = dict_find(dict_cache, key);
    if (entry == NULL) {
        *cache_key = key;
        return false;
    }

    struct cache_val *cache = entry->val;
    double now = current_timestamp();
    if ((now - cache->time) > settings.cache_timeout) {
        dict_delete(dict_cache, key);
        *cache_key = key;
        return false;
    }

    reply_result(ses, pkg, cache->result);
    sdsfree(key);
    return true;
}

static int add_cache(sds cache_key, json_t *result)
{
    struct cache_val cache;
    cache.time = current_timestamp();
    cache.result = result;
    json_incref(result);
    dict_replace(dict_cache, cache_key, &cache);

    return 0;
}

// group.list
static int on_cmd_group_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < configs.group_num; ++i) {
        json_t *group = json_object();
        json_object_set_new(group, "group", json_string(configs.groups[i].name));
        json_object_set_new(group, "leverage", json_integer(configs.groups[i].leverage));
        json_array_append_new(result, group);
    }
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// symbol.list
static int on_cmd_symbol_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < configs.symbol_num; ++i) {
        json_t *symbol = json_object();
        json_object_set_new(symbol, "symbol", json_string(configs.symbols[i].name));
        json_object_set_new(symbol, "security", json_string(configs.symbols[i].security));
        json_object_set_new(symbol, "digit", json_integer(configs.symbols[i].digit));
        json_object_set_new(symbol, "currency", json_string(configs.symbols[i].currency));
        json_object_set_new_mpd(symbol, "contract_size", configs.symbols[i].contract_size);
        json_object_set_new_mpd(symbol, "percentage", configs.symbols[i].percentage);

        // get symbol fee
        json_t *fees = json_object();
        char *name = configs.symbols[i].name;
        for (int j = 0; j < configs.group_num; ++j) {
            char *grp = configs.groups[j].name;
            json_object_set_new_mpd(fees, grp, symbol_fee(grp, name));
        }
        json_object_set_new(symbol, "fees", fees);

        // get symbol long swap
        json_t *longs = json_object();
        for (int j = 0; j < configs.group_num; ++j) {
            char *grp = configs.groups[j].name;
            json_object_set_new_mpd(longs, grp, symbol_swap_long(grp, name));
        }
        json_object_set_new(symbol, "long_swaps", longs);

        // get symbol short swap
        json_t *shorts = json_object();
        for (int j = 0; j < configs.group_num; ++j) {
            char *grp = configs.groups[j].name;
            json_object_set_new_mpd(shorts, grp, symbol_swap_short(grp, name));
        }
        json_object_set_new(symbol, "short_swaps", shorts);

        json_object_set_new(symbol, "margin_type", json_integer(configs.symbols[i].margin_type));
        json_object_set_new(symbol, "profit_type", json_integer(configs.symbols[i].profit_type));
        json_object_set_new(symbol, "margin_calc", json_integer(configs.symbols[i].margin_calc));
        json_object_set_new(symbol, "profit_calc", json_integer(configs.symbols[i].profit_calc));
        json_object_set_new_mpd(symbol, "tick_size", configs.symbols[i].tick_size);
        json_object_set_new_mpd(symbol, "tick_price", configs.symbols[i].tick_price);
        json_array_append_new(result, symbol);
    }
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// tick.status
static int on_cmd_tick_status(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_object();
    int status = tick_status();
    json_object_set_new(result, "status", json_integer(tick_status()));
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// balance.query (sid)
static int on_cmd_balance_query_v2(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size != 1)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));
    if (sid == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();

    mpd_t *balance = balance_get_v2(sid, BALANCE_TYPE_BALANCE);
    if (balance) {
        json_object_set_new_mpd(result, "balance", balance);
    } else {
        json_object_set_new(result, "balance", json_string("0"));
        json_object_set_new(result, "equity", json_string("0"));
        json_object_set_new(result, "margin", json_string("0"));
        json_object_set_new(result, "margin_free", json_string("0"));

        int ret = reply_result(ses, pkg, result);
        json_decref(result);
        return ret;
    }

    mpd_t *pnl = balance_get_v2(sid, BALANCE_TYPE_FLOAT);
    if (pnl) {
        json_object_set_new_mpd(result, "pnl", pnl);
    } else {
        json_object_set_new(result, "pnl", json_string("0"));
    }

    mpd_t *equity = balance_get_v2(sid, BALANCE_TYPE_EQUITY);
    if (equity) {
        json_object_set_new_mpd(result, "equity", equity);
    } else {
        json_object_set_new(result, "equity", json_string("0"));
    }

    mpd_t *margin = balance_get_v2(sid, BALANCE_TYPE_MARGIN);
    if (margin) {
        json_object_set_new_mpd(result, "margin", margin);
    } else {
        json_object_set_new(result, "margin", json_string("0"));
    }

    mpd_t *margin_free = balance_get_v2(sid, BALANCE_TYPE_FREE);
    if (margin_free) {
        json_object_set_new_mpd(result, "margin_free", margin_free);
    } else {
        json_object_set_new(result, "margin_fee", json_string("0"));
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// balance.update (sid, change, comment)
static int on_cmd_balance_update_v2(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // change
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 1)), PREC_DEFAULT);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // comment
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *comment = json_string_value(json_array_get(params, 2));

    int ret = update_user_balance_v2(true, sid, change, comment);
    mpd_del(change);
    if (ret == -2) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_balance", params);
    return reply_success(ses, pkg);
}

// order.open2 (sid, group, symbol, side, price, lot, tp, sl, comment, margin_price)
static int on_cmd_order_open2(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 10)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return reply_error_invalid_argument(ses, pkg);

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return reply_error_invalid_argument(ses, pkg);

    // price
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    double dd = atof(json_string_value(json_array_get(params, 4)));
    if (dd == 0)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *lot = mpd_new(&mpd_ctx);
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);
    mpd_t *margin_price = mpd_new(&mpd_ctx);
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_t *swap = mpd_new(&mpd_ctx);

    price = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // lot
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 5)));
    if (dd == 0)
        goto invalid_argument;

    lot = decimal(json_string_value(json_array_get(params, 5)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // tp
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    }
    if (tp == NULL || mpd_cmp(tp, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    // sl
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 7)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 7)), PREC_PRICE);
    }
    if (sl == NULL || mpd_cmp(sl, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    // comment
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 8));

    // margin price
    if (!json_is_string(json_array_get(params, 9)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 9)));
    if (dd == 0)
        goto invalid_argument;

    margin_price = decimal(json_string_value(json_array_get(params, 9)), PREC_PRICE);
    if (margin_price == NULL || mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // get symbol fee and swap
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

    json_t *result = NULL;
    int ret = market_open_hedged(true, &result, market, get_symbol(symbol), sid, leverage, side, price, lot, tp, sl, symbol_percentage(group, symbol), fee, swap, 0, comment, margin_price, 0);

    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(margin_price);
    mpd_del(fee);
    mpd_del(swap);

    if (ret == -2) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret < 0) {
        log_fatal("market_open fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    // 添加参数create_time,系统重启时创建订单使用
    json_t *create_time = json_object_get(result, "create_time");
    json_array_append_new(params, create_time);

    append_operlog("open_order2", params);
    ret = reply_result(ses, pkg, result);
    json_decref(create_time);
    json_decref(result);
    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (lot)
        mpd_del(lot);
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);
    if (margin_price)
        mpd_del(margin_price);
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);

    return reply_error_invalid_argument(ses, pkg);
}

// order.position (sid)
static int on_cmd_order_position(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    json_t *result = json_object();
    json_t *orders = json_array();
    uint32_t total = 0;

    for (int i = 0; i < configs.symbol_num; ++i) {
        market_t *market = get_market(configs.symbols[i].name);
        skiplist_t *order_list = market_get_order_list_v2(market, sid);
        if (order_list != NULL) {
            total += order_list->len;
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            while ((node = skiplist_next(iter)) != NULL) {
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info_v2(order));
            }
            skiplist_release_iterator(iter);
        }
    }
    json_object_set_new(result, "total", json_integer(total));

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// order.close2 (sid, symbol, order_id, price, comment, profit_price)
static int on_cmd_order_close2(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 16, "order not found");
    }
    if (order->sid != sid) {
        return reply_error(ses, pkg, 17, "user not match");
    }

    // price
    if (!json_is_string(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    double dd = atof(json_string_value(json_array_get(params, 3)));
    if (dd == 0)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *profit_price = mpd_new(&mpd_ctx);

    price = decimal(json_string_value(json_array_get(params, 3)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // comment
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 4));

    // profit price
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 5)));
    if (dd == 0)
        goto invalid_argument;

    profit_price = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    if (profit_price == NULL || mpd_cmp(profit_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    json_t *result = NULL;
    int ret = market_close(true, &result, market, get_symbol(symbol), sid, order, price, comment, profit_price, 0);

    mpd_del(price);
    mpd_del(profit_price);

    if (ret < 0) {
        log_fatal("market_close fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    // 添加参数finish_time,系统重启时平仓使用
    json_t *finish_time = json_object_get(result, "finish_time");
    json_array_append_new(params, finish_time);

    append_operlog("close_order2", params);
    ret = reply_result(ses, pkg, result);
    json_decref(finish_time);
    json_decref(result);
    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return reply_error_invalid_argument(ses, pkg);
}

// order.open (sid, group, symbol, side, lot, tp, sl, external, comment)
static int on_cmd_order_open(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 9)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return reply_error_invalid_argument(ses, pkg);

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    bool time_in_range = false;
    time_in_range = symbol_check_time_in_range(symbol);
    if (!time_in_range) {
        return reply_error_market_close(ses, pkg);
    }

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return reply_error_invalid_argument(ses, pkg);

    // lot
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    double dd = atof(json_string_value(json_array_get(params, 4)));
    if (dd == 0)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *bid = mpd_new(&mpd_ctx);
    mpd_t *ask = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *lot = mpd_new(&mpd_ctx);
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);
    mpd_t *margin_price = mpd_new(&mpd_ctx);
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_t *swap = mpd_new(&mpd_ctx);

    lot = decimal(json_string_value(json_array_get(params, 4)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price
    mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
    mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(price, ask, &mpd_ctx);
    } else {
        mpd_copy(price, bid, &mpd_ctx);
    }

    // tp
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 5)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    }
    if (tp == NULL || mpd_cmp(tp, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(tp, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(tp, ask, &mpd_ctx) <= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(tp, bid, &mpd_ctx) >= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            mpd_del(tp);
            return reply_error(ses, pkg, 12, "invalid take profit");
        }
    }

    // sl
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    }
    if (sl == NULL || mpd_cmp(sl, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(sl, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(sl, bid, &mpd_ctx) >= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(sl, ask, &mpd_ctx) <= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            mpd_del(tp);
            mpd_del(sl);
            return reply_error(ses, pkg, 13, "invalid stop loss");
        }
    }

    // external
    if (!json_is_integer(json_array_get(params, 7)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t external = json_integer_value(json_array_get(params, 7));

    // comment
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 8));

    // margin price
    mpd_copy(margin_price, mpd_one, &mpd_ctx);
    symbol_t *sym = get_symbol(symbol);
    if (sym->margin_calc == MARGIN_CALC_FOREX) {
        if (sym->margin_type == MARGIN_TYPE_AC || sym->margin_type == MARGIN_TYPE_BC) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(margin_price, symbol_ask(sym->margin_symbol), &mpd_ctx);
            } else {
                mpd_copy(margin_price, symbol_bid(sym->margin_symbol), &mpd_ctx);
            }
        }
    } else {
        if (strcmp(sym->name, "HSI") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(margin_price, symbol_ask("USDHKD"), &mpd_ctx);
            } else {
                mpd_copy(margin_price, symbol_bid("USDHKD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "DAX") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(margin_price, symbol_ask("EURUSD"), &mpd_ctx);
            } else {
                mpd_copy(margin_price, symbol_bid("EURUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "UK100") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(margin_price, symbol_ask("GBPUSD"), &mpd_ctx);
            } else {
                mpd_copy(margin_price, symbol_bid("GBPUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "JP225") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(margin_price, symbol_ask("USDJPY"), &mpd_ctx);
            } else {
                mpd_copy(margin_price, symbol_bid("USDJPY"), &mpd_ctx);
            }
        }
    }

    // get symbol fee and swap
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

    double create_time = current_timestamp();
    json_t *result = NULL;
//    int ret = market_open(true, &result, market, sym, sid, leverage, side, price, lot, tp, sl, fee, swap, external, comment, margin_price, create_time);
    int ret = market_open_hedged(true, &result, market, sym, sid, leverage, side, price, lot, tp, sl, symbol_percentage(group, symbol), fee, swap, external, comment, margin_price, create_time);

    if (ret == 0) {
        // 添加参数 price, margin_time, create_time,系统重启时创建订单使用
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(price, 0))));
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(margin_price, 0))));
        json_array_append_new(params, json_real(create_time));
    }

    mpd_del(bid);
    mpd_del(ask);
    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(margin_price);
    mpd_del(fee);
    mpd_del(swap);

    if (ret == -2) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 11, "symbol price is 0");
    } else if (ret == -4) {
        return reply_error(ses, pkg, 15, "margin symbol price is 0");
    } else if (ret < 0) {
        log_fatal("market_open fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("open_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (bid)
        mpd_del(bid);
    if (ask)
        mpd_del(ask);
    if (price)
        mpd_del(price);
    if (lot)
        mpd_del(lot);
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);
    if (margin_price)
        mpd_del(margin_price);
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);

    return reply_error_invalid_argument(ses, pkg);
}

// order.close (sid, symbol, order_id, comment)
static int on_cmd_order_close(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));

    // check open close time
    bool time_in_range = symbol_check_time_in_range(symbol);
    if (!time_in_range) {
        return reply_error_market_close(ses, pkg);
    }

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 16, "order not found");
    }
    if (order->sid != sid) {
        return reply_error(ses, pkg, 17, "user not match");
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 3));

    uint32_t side = order->side;
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *profit_price = mpd_new(&mpd_ctx);

    // price
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(price, symbol_bid(symbol), &mpd_ctx);
    } else {
        mpd_copy(price, symbol_ask(symbol), &mpd_ctx);
    }

    // profit price
    mpd_copy(profit_price, mpd_one, &mpd_ctx);
    symbol_t *sym = get_symbol(symbol);
    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        if (sym->profit_type == PROFIT_TYPE_AC || sym->profit_type == PROFIT_TYPE_CB) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid(sym->profit_symbol), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask(sym->profit_symbol), &mpd_ctx);
            }
        }
    } else {
        if (strcmp(sym->name, "HSI") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid("USDHKD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask("USDHKD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "DAX") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid("EURUSD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask("EURUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "UK100") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_ask("GBPUSD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_bid("GBPUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "JP225") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_ask("USDJPY"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_bid("USDJPY"), &mpd_ctx);
            }
        }
    }

    double finish_time = current_timestamp();
    json_t *result = NULL;
//    int ret = market_close(true, &result, market, sym, sid, order, price, comment, profit_price, finish_time);
    int ret = market_close_hedged(true, &result, market, sym, sid, order, price, comment, profit_price, finish_time);

    if (ret == 0) {
        // 添加参数 price, profit_price, finish_time, 系统重启时平仓使用
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(price, 0))));
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(profit_price, 0))));
        json_array_append_new(params, json_real(finish_time));
    }

    mpd_del(price);
    mpd_del(profit_price);

    if (ret == -3) {
        return reply_error(ses, pkg, 11, "symbol price is 0");
    } else if (ret == -5) {
        return reply_error(ses, pkg, 18, "profit symbol price is 0");
    } else if (ret < 0) {
        log_fatal("market_close fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("close_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return reply_error_invalid_argument(ses, pkg);
}

// order.update (sid, symbol, order_id, tp, sl)
static int on_cmd_order_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 16, "order not found");
    }
    if (order->sid != sid) {
        return reply_error(ses, pkg, 17, "user not match");
    }

    int side = order->side;
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);
    mpd_t *bid = mpd_new(&mpd_ctx);
    mpd_t *ask = mpd_new(&mpd_ctx);

    // price
    mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
    mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);

    // tp
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    double dd = atof(json_string_value(json_array_get(params, 3)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 3)), PREC_PRICE);
    }
    if (tp == NULL || mpd_cmp(tp, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(tp, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(tp, ask, &mpd_ctx) <= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(tp, bid, &mpd_ctx) >= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(tp);
            return reply_error(ses, pkg, 12, "invalid take profit");
        }
    }

    // sl
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 4)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    }
    if (sl == NULL || mpd_cmp(sl, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(sl, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(sl, bid, &mpd_ctx) >= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(sl, ask, &mpd_ctx) <= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(tp);
            mpd_del(sl);
            return reply_error(ses, pkg, 13, "invalid stop loss");
        }
    }

    json_t *result = NULL;
    int ret = market_update(true, &result, market, order, tp, sl);

    mpd_del(tp);
    mpd_del(sl);
    mpd_del(bid);
    mpd_del(ask);

    if (ret < 0) {
        log_fatal("market_update fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);
    if (bid)
        mpd_del(bid);
    if (ask)
        mpd_del(ask);

    return reply_error_invalid_argument(ses, pkg);
}

// order.limit (sid, group, symbol, side, lot, price, tp, sl, expire, external, comment)
static int on_cmd_order_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 12)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return reply_error_invalid_argument(ses, pkg);

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return reply_error_invalid_argument(ses, pkg);

    // lot
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    double dd = atof(json_string_value(json_array_get(params, 4)));
    if (dd == 0)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *bid = mpd_new(&mpd_ctx);
    mpd_t *ask = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *lot = mpd_new(&mpd_ctx);
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_t *swap = mpd_new(&mpd_ctx);

    lot = decimal(json_string_value(json_array_get(params, 4)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // ask, bid
    mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
    mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);

    // type
    if (!json_is_integer(json_array_get(params, 5)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t type = json_integer_value(json_array_get(params, 5));
    if(type != MARKET_ORDER_TYPE_LIMIT && type != MARKET_ORDER_TYPE_BREAK)
    {
        goto invalid_argument;
    }

    // price
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0)
        goto invalid_argument;

    price = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (side == ORDER_SIDE_BUY) {
        if ((mpd_cmp(price, ask, &mpd_ctx) > 0 && type == MARKET_ORDER_TYPE_LIMIT ) ||
                (mpd_cmp(price, ask, &mpd_ctx) < 0 &&type == MARKET_ORDER_TYPE_BREAK)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            return reply_error(ses, pkg, 19, "invalid price");
        }
    } else {
        if ((mpd_cmp(price, bid, &mpd_ctx) < 0 && type == MARKET_ORDER_TYPE_LIMIT ) ||
                (mpd_cmp(price, bid, &mpd_ctx) > 0 &&type == MARKET_ORDER_TYPE_BREAK)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            return reply_error(ses, pkg, 19, "invalid price");
        }
    }

    // tp
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 7)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 7)), PREC_PRICE);
    }
    if (tp == NULL || mpd_cmp(tp, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(tp, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(tp, price, &mpd_ctx) <= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(tp, price, &mpd_ctx) >= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            mpd_del(tp);
            return reply_error(ses, pkg, 12, "invalid take profit");
        }
    }

    // sl
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 7)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 7)), PREC_PRICE);
    }
    if (sl == NULL || mpd_cmp(sl, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(sl, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(sl, price, &mpd_ctx) >= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(sl, price, &mpd_ctx) <= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(price);
            mpd_del(lot);
            mpd_del(tp);
            mpd_del(sl);
            return reply_error(ses, pkg, 13, "invalid stop loss");
        }
    }

    // expire_time
    uint64_t expire_time = json_integer_value(json_array_get(params, 9));
    if (expire_time > 0 && expire_time < current_timestamp())
        goto invalid_argument;

    // external
    if (!json_is_integer(json_array_get(params, 10)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t external = json_integer_value(json_array_get(params, 10));


    // comment
    if (!json_is_string(json_array_get(params, 11)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 11));

    // get symbol fee and swap
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

    double create_time = current_timestamp();
    json_t *result = NULL;
    int ret = market_put_limit(true, &result, market, sid, leverage, side, price, lot, tp, sl, symbol_percentage(group, symbol), fee, swap, external, comment, create_time, expire_time, type);

    if (ret == 0) {
        // 添加参数 create_time,系统重启时创建订单使用
        json_array_append_new(params, json_real(create_time));
    }

    mpd_del(bid);
    mpd_del(ask);
    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(fee);
    mpd_del(swap);

    if (ret < 0) {
        log_fatal("order limit fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("limit_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (bid)
        mpd_del(bid);
    if (ask)
        mpd_del(ask);
    if (price)
        mpd_del(price);
    if (lot)
        mpd_del(lot);
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);
//    if (margin_price)
//        mpd_del(margin_price);
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);

    return reply_error_invalid_argument(ses, pkg);
}

// order.pending (sid)
static int on_cmd_order_pending(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 1)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    json_t *result = json_object();
    json_t *orders = json_array();
    uint32_t total = 0;

    for (int i = 0; i < configs.symbol_num; ++i) {
        market_t *market = get_market(configs.symbols[i].name);
        skiplist_t *order_list = market_get_limit_list(market, sid);
        if (order_list != NULL) {
            total += order_list->len;
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            while ((node = skiplist_next(iter)) != NULL) {
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info_v2(order));
            }
            skiplist_release_iterator(iter);
        }
    }
    json_object_set_new(result, "total", json_integer(total));

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// order.cancel (sid, symbol, order_id, comment)
static int on_cmd_order_cancel_v2(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    const char *comment = json_string_value(json_array_get(params, 3));

    order_t *order = market_get_limit(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (order->sid != sid) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    double finish_time = current_timestamp();
    json_t *result = NULL;
    int ret = market_cancel(true, &result, market, order, comment, finish_time);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    } else {
        json_array_append_new(params, json_real(finish_time));
    }

    append_operlog("cancel_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

// order.close_external (sid, symbol, external, comment)
static int on_cmd_order_close_external(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t external = json_integer_value(json_array_get(params, 2));
    if (external == 0)
        return reply_error_invalid_argument(ses, pkg);

    order_t *order = market_get_external_order(market, sid, external);
    if (order == NULL) {
        return reply_error(ses, pkg, 16, "order not found");
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 3));

    uint32_t side = order->side;
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *profit_price = mpd_new(&mpd_ctx);

    // price
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(price, symbol_bid(symbol), &mpd_ctx);
    } else {
        mpd_copy(price, symbol_ask(symbol), &mpd_ctx);
    }

    // profit price
    mpd_copy(profit_price, mpd_one, &mpd_ctx);
    symbol_t *sym = get_symbol(symbol);
    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        if (sym->profit_type == PROFIT_TYPE_AC || sym->profit_type == PROFIT_TYPE_CB) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid(sym->profit_symbol), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask(sym->profit_symbol), &mpd_ctx);
            }
        }
    } else {
        if (strcmp(sym->name, "HSI") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid("USDHKD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask("USDHKD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "DAX") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_bid("EURUSD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_ask("EURUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "UK100") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_ask("GBPUSD"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_bid("GBPUSD"), &mpd_ctx);
            }
        } else if (strcmp(sym->name, "JP225") == 0) {
            if (side == ORDER_SIDE_BUY) {
                mpd_copy(profit_price, symbol_ask("USDJPY"), &mpd_ctx);
            } else {
                mpd_copy(profit_price, symbol_bid("USDJPY"), &mpd_ctx);
            }
        }
    }

    double finish_time = current_timestamp();
    json_t *result = NULL;
//    int ret = market_close(true, &result, market, sym, sid, order, price, comment, profit_price, finish_time);
    int ret = market_close_hedged(true, &result, market, sym, sid, order, price, comment, profit_price, finish_time);

    if (ret == 0) {
        // 添加参数 price, profit_price, finish_time, 系统重启时平仓使用
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(price, 0))));
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(profit_price, 0))));
        json_array_append_new(params, json_real(finish_time));
    }

    mpd_del(price);
    mpd_del(profit_price);

    if (ret == -3) {
        return reply_error(ses, pkg, 11, "symbol price is 0");
    } else if (ret == -5) {
        return reply_error(ses, pkg, 18, "profit symbol price is 0");
    } else if (ret < 0) {
        log_fatal("market_close fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("close_external_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return reply_error_invalid_argument(ses, pkg);
}

// order.update_external (sid, symbol, external, tp, sl)
static int on_cmd_order_update_external(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 5)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t external = json_integer_value(json_array_get(params, 2));
    if (external == 0)
        return reply_error_invalid_argument(ses, pkg);

    order_t *order = market_get_external_order(market, sid, external);
    if (order == NULL) {
        return reply_error(ses, pkg, 16, "order not found");
    }

    int side = order->side;
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);
    mpd_t *bid = mpd_new(&mpd_ctx);
    mpd_t *ask = mpd_new(&mpd_ctx);

    // price
    mpd_copy(bid, symbol_bid(symbol), &mpd_ctx);
    mpd_copy(ask, symbol_ask(symbol), &mpd_ctx);

    // tp
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    double dd = atof(json_string_value(json_array_get(params, 3)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 3)), PREC_PRICE);
    }
    if (tp == NULL || mpd_cmp(tp, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(tp, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(tp, ask, &mpd_ctx) <= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(tp, bid, &mpd_ctx) >= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(tp);
            return reply_error(ses, pkg, 12, "invalid take profit");
        }
    }

    // sl
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    dd = atof(json_string_value(json_array_get(params, 4)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    }
    if (sl == NULL || mpd_cmp(sl, mpd_zero, &mpd_ctx) < 0)
        goto invalid_argument;

    if (mpd_cmp(sl, mpd_zero, &mpd_ctx) > 0) {
        if ((side == ORDER_SIDE_BUY && mpd_cmp(sl, bid, &mpd_ctx) >= 0) ||
            (side == ORDER_SIDE_SELL && mpd_cmp(sl, ask, &mpd_ctx) <= 0)) {
            mpd_del(bid);
            mpd_del(ask);
            mpd_del(tp);
            mpd_del(sl);
            return reply_error(ses, pkg, 13, "invalid stop loss");
        }
    }

    json_t *result = NULL;
    int ret = market_update(true, &result, market, order, tp, sl);

    mpd_del(tp);
    mpd_del(sl);
    mpd_del(bid);
    mpd_del(ask);

    if (ret < 0) {
        log_fatal("market_update fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_external_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);
    if (bid)
        mpd_del(bid);
    if (ask)
        mpd_del(ask);

    return reply_error_invalid_argument(ses, pkg);
}

// order.cancel_external (sid, symbol, external, comment)
static int on_cmd_order_cancel_external(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *symbol = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(symbol);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t external = json_integer_value(json_array_get(params, 2));

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    const char *comment = json_string_value(json_array_get(params, 3));

    order_t *order = market_get_external_limit(market, sid, external);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (order->sid != sid) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    double finish_time = current_timestamp();
    json_t *result = NULL;
    int ret = market_cancel(true, &result, market, order, comment, finish_time);
    if (ret < 0) {
        log_fatal("cancel external order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    } else {
        json_array_append_new(params, json_real(finish_time));
    }

    append_operlog("cancel_external_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_balance_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    size_t request_size = json_array_size(params);
    if (request_size == 0)
        return reply_error_invalid_argument(ses, pkg);

    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));
    if (user_id == 0)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
/*
    if (request_size == 1) {
        for (size_t i = 0; i < settings.asset_num; ++i) {
            const char *asset = settings.assets[i].name;
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }

            json_object_set_new(result, asset, unit);
        }
    } else {
        for (size_t i = 1; i < request_size; ++i) {
            const char *asset = json_string_value(json_array_get(params, i));
            if (!asset || !asset_exist(asset)) {
                json_decref(result);
                return reply_error_invalid_argument(ses, pkg);
            }
            json_t *unit = json_object();
            int prec_save = asset_prec(asset);
            int prec_show = asset_prec_show(asset);

            mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE, asset);
            if (available) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(available);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "available", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "available", available);
                }
            } else {
                json_object_set_new(unit, "available", json_string("0"));
            }

            mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE, asset);
            if (freeze) {
                if (prec_save != prec_show) {
                    mpd_t *show = mpd_qncopy(freeze);
                    mpd_rescale(show, show, -prec_show, &mpd_ctx);
                    json_object_set_new_mpd(unit, "freeze", show);
                    mpd_del(show);
                } else {
                    json_object_set_new_mpd(unit, "freeze", freeze);
                }
            } else {
                json_object_set_new(unit, "freeze", json_string("0"));
            }

            json_object_set_new(result, asset, unit);
        }
    }
*/
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_balance_update(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *asset = json_string_value(json_array_get(params, 1));

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), 2);
    if (change == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return reply_error_invalid_argument(ses, pkg);
    }

    int ret = update_user_balance(true, user_id, asset, business, business_id, change, detail);
    mpd_del(change);
    if (ret == -1) {
        return reply_error(ses, pkg, 9, "repeat update");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret < 0) {
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("update_balance", params);
    return reply_success(ses, pkg);
}

/*
static int on_cmd_order_put_limit(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 8)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount    = NULL;
    mpd_t *price     = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price 
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL || mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    json_t *result = NULL;
    int ret = market_put_limit_order(true, &result, market, user_id, side, amount, price, taker_fee, maker_fee, source);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("limit_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);

    return reply_error_invalid_argument(ses, pkg);
}

static int on_cmd_order_put_market(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 6)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL || mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL || mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto invalid_argument;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) >= SOURCE_MAX_LEN)
        goto invalid_argument;

    json_t *result = NULL;
    int ret = market_put_market_order(true, &result, market, user_id, side, amount, taker_fee, source);

    mpd_del(amount);
    mpd_del(taker_fee);

    if (ret == -1) {
        return reply_error(ses, pkg, 10, "balance not enough");
    } else if (ret == -2) {
        return reply_error(ses, pkg, 11, "amount too small");
    } else if (ret == -3) {
        return reply_error(ses, pkg, 12, "no enough trader");
    } else if (ret < 0) {
        log_fatal("market_put_limit_order fail: %d", ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("market_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);

    return reply_error_invalid_argument(ses, pkg);
}
*/

static int on_cmd_order_query(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_LIST_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "limit", json_integer(limit));
    json_object_set_new(result, "offset", json_integer(offset));

    json_t *orders = json_array();
    skiplist_t *order_list = market_get_order_list(market, user_id);
    if (order_list == NULL) {
        json_object_set_new(result, "total", json_integer(0));
    } else {
        json_object_set_new(result, "total", json_integer(order_list->len));
        if (offset < order_list->len) {
            skiplist_iter *iter = skiplist_get_iterator(order_list);
            skiplist_node *node;
            for (size_t i = 0; i < offset; i++) {
                if (skiplist_next(iter) == NULL)
                    break;
            }
            size_t index = 0;
            while ((node = skiplist_next(iter)) != NULL && index < limit) {
                index++;
                order_t *order = node->value;
                json_array_append_new(orders, get_order_info(order));
            }
            skiplist_release_iterator(iter);
        }
    }

    json_object_set_new(result, "records", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_cancel(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return reply_error(ses, pkg, 10, "order not found");
    }
    if (order->user_id != user_id) {
        return reply_error(ses, pkg, 11, "user not match");
    }

    json_t *result = NULL;
    int ret = market_cancel_order(true, &result, market, order);
    if (ret < 0) {
        log_fatal("cancel order: %"PRIu64" fail: %d", order->id, ret);
        return reply_error_internal_error(ses, pkg);
    }

    append_operlog("cancel_order", params);
    ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_order_book(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 4)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // side
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint32_t side = json_integer_value(json_array_get(params, 1));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return reply_error_invalid_argument(ses, pkg);

    // offset
    if (!json_is_integer(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    size_t offset = json_integer_value(json_array_get(params, 2));

    // limit
    if (!json_is_integer(json_array_get(params, 3)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 3));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    json_t *result = json_object();
    json_object_set_new(result, "offset", json_integer(offset));
    json_object_set_new(result, "limit", json_integer(limit));

    uint64_t total;
    skiplist_iter *iter;
    if (side == MARKET_ORDER_SIDE_ASK) {
        iter = skiplist_get_iterator(market->asks);
        total = market->asks->len;
        json_object_set_new(result, "total", json_integer(total));
    } else {
        iter = skiplist_get_iterator(market->bids);
        total = market->bids->len;
        json_object_set_new(result, "total", json_integer(total));
    }

    json_t *orders = json_array();
    if (offset < total) {
        for (size_t i = 0; i < offset; i++) {
            if (skiplist_next(iter) == NULL)
                break;
        }
        size_t index = 0;
        skiplist_node *node;
        while ((node = skiplist_next(iter)) != NULL && index < limit) {
            index++;
            order_t *order = node->value;
            json_array_append_new(orders, get_order_info(order));
        }
    }
    skiplist_release_iterator(iter);

    json_object_set_new(result, "orders", orders);
    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static json_t *get_depth(market_t *market, size_t limit)
{
/*
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_copy(price, order->price, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) == 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);
*/
    json_t *result = json_object();
    return result;
}

static json_t *get_depth_merge(market_t* market, size_t limit, mpd_t *interval)
{
/*
    mpd_t *q = mpd_new(&mpd_ctx);
    mpd_t *r = mpd_new(&mpd_ctx);
    mpd_t *price = mpd_new(&mpd_ctx);
    mpd_t *amount = mpd_new(&mpd_ctx);

    json_t *asks = json_array();
    skiplist_iter *iter = skiplist_get_iterator(market->asks);
    skiplist_node *node = skiplist_next(iter);
    size_t index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        if (mpd_cmp(r, mpd_zero, &mpd_ctx) != 0) {
            mpd_add(price, price, interval, &mpd_ctx);
        }
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) >= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }
        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(asks, info);
    }
    skiplist_release_iterator(iter);

    json_t *bids = json_array();
    iter = skiplist_get_iterator(market->bids);
    node = skiplist_next(iter);
    index = 0;
    while (node && index < limit) {
        index++;
        order_t *order = node->value;
        mpd_divmod(q, r, order->price, interval, &mpd_ctx);
        mpd_mul(price, q, interval, &mpd_ctx);
        mpd_copy(amount, order->left, &mpd_ctx);
        while ((node = skiplist_next(iter)) != NULL) {
            order = node->value;
            if (mpd_cmp(price, order->price, &mpd_ctx) <= 0) {
                mpd_add(amount, amount, order->left, &mpd_ctx);
            } else {
                break;
            }
        }

        json_t *info = json_array();
        json_array_append_new_mpd(info, price);
        json_array_append_new_mpd(info, amount);
        json_array_append_new(bids, info);
    }
    skiplist_release_iterator(iter);

    mpd_del(q);
    mpd_del(r);
    mpd_del(price);
    mpd_del(amount);

    json_t *result = json_object();
    json_object_set_new(result, "asks", asks);
    json_object_set_new(result, "bids", bids);
*/
    json_t *result = json_object();
    return result;
}

/*
static int on_cmd_order_book_depth(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 3)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // limit
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    size_t limit = json_integer_value(json_array_get(params, 1));
    if (limit > ORDER_BOOK_MAX_LEN)
        return reply_error_invalid_argument(ses, pkg);

    // interval
    if (!json_is_string(json_array_get(params, 2)))
        return reply_error_invalid_argument(ses, pkg);
    mpd_t *interval = decimal(json_string_value(json_array_get(params, 2)), market->money_prec);
    if (!interval)
        return reply_error_invalid_argument(ses, pkg);
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) < 0) {
        mpd_del(interval);
        return reply_error_invalid_argument(ses, pkg);
    }

    sds cache_key = NULL;
    if (process_cache(ses, pkg, &cache_key)) {
        mpd_del(interval);
        return 0;
    }

    json_t *result = NULL;
    if (mpd_cmp(interval, mpd_zero, &mpd_ctx) == 0) {
        result = get_depth(market, limit);
    } else {
        result = get_depth_merge(market, limit, interval);
    }
    mpd_del(interval);

    if (result == NULL) {
        sdsfree(cache_key);
        return reply_error_internal_error(ses, pkg);
    }

    add_cache(cache_key, result);
    sdsfree(cache_key);

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}
*/

static int on_cmd_order_detail(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    if (json_array_size(params) != 2)
        return reply_error_invalid_argument(ses, pkg);

    // market
    if (!json_is_string(json_array_get(params, 0)))
        return reply_error_invalid_argument(ses, pkg);
    const char *market_name = json_string_value(json_array_get(params, 0));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return reply_error_invalid_argument(ses, pkg);

    // order_id
    if (!json_is_integer(json_array_get(params, 1)))
        return reply_error_invalid_argument(ses, pkg);
    uint64_t order_id = json_integer_value(json_array_get(params, 1));

    order_t *order = market_get_order(market, order_id);
    json_t *result = NULL;
    if (order == NULL) {
        result = json_null();
    } else {
        result = get_order_info(order);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static int on_cmd_market_list(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    for (int i = 0; i < settings.market_num; ++i) {
        json_t *market = json_object();
        json_object_set_new(market, "name", json_string(settings.markets[i].name));
        json_object_set_new(market, "stock", json_string(settings.markets[i].stock));
        json_object_set_new(market, "money", json_string(settings.markets[i].money));
        json_object_set_new(market, "fee_prec", json_integer(settings.markets[i].fee_prec));
        json_object_set_new(market, "stock_prec", json_integer(settings.markets[i].stock_prec));
        json_object_set_new(market, "money_prec", json_integer(settings.markets[i].money_prec));
        json_object_set_new_mpd(market, "min_amount", settings.markets[i].min_amount);
        json_array_append_new(result, market);
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;
}

static json_t *get_market_summary(const char *name)
{
    size_t ask_count;
    size_t bid_count;
    mpd_t *ask_amount = mpd_new(&mpd_ctx);
    mpd_t *bid_amount = mpd_new(&mpd_ctx);
    market_t *market = get_market(name);
    market_get_status(market, &ask_count, ask_amount, &bid_count, bid_amount);
    
    json_t *obj = json_object();
    json_object_set_new(obj, "name", json_string(name));
    json_object_set_new(obj, "ask_count", json_integer(ask_count));
    json_object_set_new_mpd(obj, "ask_amount", ask_amount);
    json_object_set_new(obj, "bid_count", json_integer(bid_count));
    json_object_set_new_mpd(obj, "bid_amount", bid_amount);

    mpd_del(ask_amount);
    mpd_del(bid_amount);

    return obj;
}

static int on_cmd_market_summary(nw_ses *ses, rpc_pkg *pkg, json_t *params)
{
    json_t *result = json_array();
    if (json_array_size(params) == 0) {
        for (int i = 0; i < settings.market_num; ++i) {
            json_array_append_new(result, get_market_summary(settings.markets[i].name));
        }
    } else {
        for (int i = 0; i < json_array_size(params); ++i) {
            const char *market = json_string_value(json_array_get(params, i));
            if (market == NULL)
                goto invalid_argument;
            if (get_market(market) == NULL)
                goto invalid_argument;
            json_array_append_new(result, get_market_summary(market));
        }
    }

    int ret = reply_result(ses, pkg, result);
    json_decref(result);
    return ret;

invalid_argument:
    json_decref(result);
    return reply_error_invalid_argument(ses, pkg);
}

static void svr_on_recv_pkg(nw_ses *ses, rpc_pkg *pkg)
{
    json_t *params = json_loadb(pkg->body, pkg->body_size, 0, NULL);
    if (params == NULL || !json_is_array(params)) {
        goto decode_error;
    }
    sds params_str = sdsnewlen(pkg->body, pkg->body_size);

    int ret;
    switch (pkg->command) {
    case CMD_BALANCE_QUERY:
        log_trace("from: %s cmd balance query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_query_v2(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_BALANCE_UPDATE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd balance update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_balance_update_v2(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_balance_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_GROUP_LIST:
        log_trace("from: %s cmd group list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_group_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_group_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_SYMBOL_LIST:
        log_trace("from: %s cmd symbol list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_symbol_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_symbol_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_TICK_STATUS:
        log_trace("from: %s cmd tick status, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_tick_status(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_tick_status %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_OPEN:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order open, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_open(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_open %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CLOSE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order close, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_close(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_close %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_POSITION:
        log_trace("from: %s cmd order position, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_position(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_position %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_OPEN2:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order open2, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_open2(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_open2 %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CLOSE2:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order close2, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_close2(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_close2 %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_UPDATE:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order update, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_update(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_update %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CLOSE_EXTERNAL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order close external, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_close_external(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_close_external %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_UPDATE_EXTERNAL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order update external, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_update_external(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_update_external %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL_EXTERNAL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel external, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel_external(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel_external %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_LIMIT:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order limit, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PENDING:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order pending, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_pending(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_pending %s fail: %d", params_str, ret);
        }
        break;
/*
    case CMD_ORDER_PUT_LIMIT:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put limit, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_limit(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_limit %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_PUT_MARKET:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order put market, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_put_market(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_put_market %s fail: %d", params_str, ret);
        }
        break;
*/
    case CMD_ORDER_QUERY:
        log_trace("from: %s cmd order query, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_query(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_query %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_CANCEL:
        if (is_operlog_block() || is_history_block() || is_message_block()) {
            log_fatal("service unavailable, operlog: %d, history: %d, message: %d",
                    is_operlog_block(), is_history_block(), is_message_block());
            reply_error_service_unavailable(ses, pkg);
            goto cleanup;
        }
        log_trace("from: %s cmd order cancel, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_cancel_v2(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_cancel %s fail: %d", params_str, ret);
        }
        break;
    case CMD_ORDER_BOOK:
        log_trace("from: %s cmd order book, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book %s fail: %d", params_str, ret);
        }
        break;
/*
    case CMD_ORDER_BOOK_DEPTH:
        log_trace("from: %s cmd order book depth, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_book_depth(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_book_depth %s fail: %d", params_str, ret);
        }
        break;
*/
    case CMD_ORDER_DETAIL:
        log_trace("from: %s cmd order detail, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_order_detail(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_order_detail %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_LIST:
        log_trace("from: %s cmd market list, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_list(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_list %s fail: %d", params_str, ret);
        }
        break;
    case CMD_MARKET_SUMMARY:
        log_trace("from: %s cmd market summary, sequence: %u params: %s", nw_sock_human_addr(&ses->peer_addr), pkg->sequence, params_str);
        ret = on_cmd_market_summary(ses, pkg, params);
        if (ret < 0) {
            log_error("on_cmd_market_summary%s fail: %d", params_str, ret);
        }
        break;
    default:
        log_error("from: %s unknown command: %u", nw_sock_human_addr(&ses->peer_addr), pkg->command);
        break;
    }

cleanup:
    sdsfree(params_str);
    json_decref(params);
    return;

decode_error:
    if (params) {
        json_decref(params);
    }
    sds hex = hexdump(pkg->body, pkg->body_size);
    log_error("connection: %s, cmd: %u decode params fail, params data: \n%s", \
            nw_sock_human_addr(&ses->peer_addr), pkg->command, hex);
    sdsfree(hex);
    rpc_svr_close_clt(svr, ses);

    return;
}

static void svr_on_new_connection(nw_ses *ses)
{
    log_trace("new connection: %s", nw_sock_human_addr(&ses->peer_addr));
}

static void svr_on_connection_close(nw_ses *ses)
{
    log_trace("connection: %s close", nw_sock_human_addr(&ses->peer_addr));
}

static uint32_t cache_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sdslen((sds)key));
}

static int cache_dict_key_compare(const void *key1, const void *key2)
{
    return sdscmp((sds)key1, (sds)key2);
}

static void *cache_dict_key_dup(const void *key)
{
    return sdsdup((const sds)key);
}

static void cache_dict_key_free(void *key)
{
    sdsfree(key);
}

static void *cache_dict_val_dup(const void *val)
{
    struct cache_val *obj = malloc(sizeof(struct cache_val));
    memcpy(obj, val, sizeof(struct cache_val));
    return obj;
}

static void cache_dict_val_free(void *val)
{
    struct cache_val *obj = val;
    json_decref(obj->result);
    free(val);
}

static void on_cache_timer(nw_timer *timer, void *privdata)
{
    dict_clear(dict_cache);
}

int init_server(void)
{
    rpc_svr_type type;
    memset(&type, 0, sizeof(type));
    type.on_recv_pkg = svr_on_recv_pkg;
    type.on_new_connection = svr_on_new_connection;
    type.on_connection_close = svr_on_connection_close;

    svr = rpc_svr_create(&settings.svr, &type);
    if (svr == NULL)
        return -__LINE__;
    if (rpc_svr_start(svr) < 0)
        return -__LINE__;

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function  = cache_dict_hash_function;
    dt.key_compare    = cache_dict_key_compare;
    dt.key_dup        = cache_dict_key_dup;
    dt.key_destructor = cache_dict_key_free;
    dt.val_dup        = cache_dict_val_dup;
    dt.val_destructor = cache_dict_val_free;

    dict_cache = dict_create(&dt, 64);
    if (dict_cache == NULL)
        return -__LINE__;

    nw_timer_set(&cache_timer, 60, true, on_cache_timer, NULL);
    nw_timer_start(&cache_timer);

    return 0;
}

