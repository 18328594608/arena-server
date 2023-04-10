# include "ut_mysql.h"
# include "me_trade.h"
# include "me_market.h"
# include "me_update.h"
# include "me_balance.h"

int load_positions(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `sid`, `side`, `create_time`, `update_time`, `symbol`, `comment`, "
                "`price`, `lot`, `margin`, `fee`, `swap`, `swaps`, `tp`, `sl`, `margin_price`, `external` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
//        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[5]);
            if (market == NULL)
                continue;

            order_t *order = malloc(sizeof(order_t));
            memset(order, 0, sizeof(order_t));
            order->id = strtoull(row[0], NULL, 0);
            order->sid = strtoull(row[1], NULL, 0);
            order->side = atoi(row[2]);
            order->create_time = strtod(row[3], NULL);
            order->update_time = strtod(row[4], NULL);
            order->symbol = strdup(row[5]);
            order->comment = strdup(row[6]);

            order->price = decimal(row[7], PREC_PRICE);
            order->lot = decimal(row[8], PREC_DEFAULT);
            order->margin = decimal(row[9], PREC_DEFAULT);
            order->fee = decimal(row[10], PREC_DEFAULT);
            order->swap = decimal(row[11], PREC_SWAP);
            order->swaps = decimal(row[12], PREC_DEFAULT);
            order->tp = decimal(row[13], PREC_PRICE);
            order->sl = decimal(row[14], PREC_PRICE);
            order->margin_price = decimal(row[15], PREC_PRICE);
            order->external = strtoull(row[16], NULL, 0);

            order->type = MARKET_ORDER_TYPE_MARKET;
            order->finish_time = 0;
            order->expire_time = 0;
            order->close_price = mpd_new(&mpd_ctx);
            order->profit      = mpd_new(&mpd_ctx);
            order->profit_price = mpd_new(&mpd_ctx);
            mpd_copy(order->close_price, mpd_zero, &mpd_ctx);
            mpd_copy(order->profit, mpd_zero, &mpd_ctx);
            mpd_copy(order->profit_price, mpd_one, &mpd_ctx);

            if (!order->symbol || !order->price || !order->lot) {
                log_error("get order detail of order id: %"PRIu64" fail", order->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            market_put_position(market, order);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_limits(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `sid`, `side`, `create_time`, `expire_time`, `symbol`, `comment`, "
                "`price`, `lot`, `margin`, `fee`, `swap`, `tp`, `sl`, `external` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[5]);
            if (market == NULL)
                continue;

            order_t *order = malloc(sizeof(order_t));
            memset(order, 0, sizeof(order_t));
            order->id = strtoull(row[0], NULL, 0);
            order->sid = strtoull(row[1], NULL, 0);
            order->side = atoi(row[2]);
            order->create_time = strtod(row[3], NULL);
            order->expire_time = strtoull(row[4], NULL, 0);
            order->symbol = strdup(row[5]);
            order->comment = strdup(row[6]);

            order->price = decimal(row[7], PREC_PRICE);
            order->lot = decimal(row[8], PREC_DEFAULT);
            order->margin = decimal(row[9], PREC_DEFAULT);
            order->fee = decimal(row[10], PREC_DEFAULT);
            order->swap = decimal(row[11], PREC_SWAP);
            order->tp = decimal(row[12], PREC_PRICE);
            order->sl = decimal(row[13], PREC_PRICE);
            order->external = strtoull(row[14], NULL, 0);

            order->type = MARKET_ORDER_TYPE_LIMIT;
            order->update_time = 0;
            order->finish_time = 0;
            order->close_price = mpd_new(&mpd_ctx);
            order->swaps = mpd_new(&mpd_ctx);
            order->profit      = mpd_new(&mpd_ctx);
            order->margin_price = mpd_new(&mpd_ctx);
            order->profit_price = mpd_new(&mpd_ctx);
            mpd_copy(order->close_price, mpd_zero, &mpd_ctx);
            mpd_copy(order->swaps, mpd_zero, &mpd_ctx);
            mpd_copy(order->profit, mpd_zero, &mpd_ctx);
            mpd_copy(order->margin_price, mpd_zero, &mpd_ctx);
            mpd_copy(order->profit_price, mpd_one, &mpd_ctx);

            if (!order->symbol || !order->price || !order->lot) {
                log_error("get order detail of order id: %"PRIu64" fail", order->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            market_put_pending(market, order);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

/*
int load_orders(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `t`, `side`, `create_time`, `update_time`, `user_id`, `market`, "
                "`price`, `amount`, `taker_fee`, `maker_fee`, `left`, `freeze`, `deal_stock`, `deal_money`, `deal_fee` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            market_t *market = get_market(row[6]);
            if (market == NULL)
                continue;

            order_t *order = malloc(sizeof(order_t));
            memset(order, 0, sizeof(order_t));
            order->id = strtoull(row[0], NULL, 0);
            order->type = strtoul(row[1], NULL, 0);
            order->side = strtoul(row[2], NULL, 0);
            order->create_time = strtod(row[3], NULL);
            order->update_time = strtod(row[4], NULL);
            order->user_id = strtoul(row[5], NULL, 0);
            order->market = strdup(row[6]);
            order->price = decimal(row[7], market->money_prec);
            order->amount = decimal(row[8], market->stock_prec);
            order->taker_fee = decimal(row[9], market->fee_prec);
            order->maker_fee = decimal(row[10], market->fee_prec);
            order->left = decimal(row[11], market->stock_prec);
            order->freeze = decimal(row[12], 0);
            order->deal_stock = decimal(row[13], 0);
            order->deal_money = decimal(row[14], 0);
            order->deal_fee = decimal(row[15], 0);

            if (!order->market || !order->price || !order->amount || !order->taker_fee || !order->maker_fee || !order->left ||
                    !order->freeze || !order->deal_stock || !order->deal_money || !order->deal_fee) {
                log_error("get order detail of order id: %"PRIu64" fail", order->id);
                mysql_free_result(result);
                return -__LINE__;
            }

            market_put_order(market, order);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}
*/

int load_balance(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `user_id`, `asset`, `t`, `balance` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            uint32_t user_id = strtoul(row[1], NULL, 0);
            const char *asset = row[2];
            uint32_t type = strtoul(row[3], NULL, 0);
            mpd_t *balance = decimal(row[4], 2);
            balance_set(user_id, type, balance);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

int load_balance_v2(MYSQL *conn, const char *table)
{
    size_t query_limit = 1000;
    uint64_t last_id = 0;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `sid`, `t`, `balance` FROM `%s` "
                "WHERE `id` > %"PRIu64" ORDER BY id LIMIT %zu", table, last_id, query_limit);
//        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            last_id = strtoull(row[0], NULL, 0);
            uint64_t sid = strtoul(row[1], NULL, 0);
            uint32_t type = strtoul(row[2], NULL, 0);
            mpd_t *balance = decimal(row[3], PREC_DEFAULT);
            balance_set_float(sid, type, balance);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    return 0;
}

/*
static int load_update_balance(json_t *params)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // asset
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *asset = json_string_value(json_array_get(params, 1));
    int prec = asset_prec(asset);
    if (prec < 0)
        return 0;

    // business
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *business = json_string_value(json_array_get(params, 2));

    // business_id
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint64_t business_id = json_integer_value(json_array_get(params, 3));

    // change
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *change = decimal(json_string_value(json_array_get(params, 4)), prec);
    if (change == NULL)
        return -__LINE__;

    // detail
    json_t *detail = json_array_get(params, 5);
    if (!json_is_object(detail)) {
        mpd_del(change);
        return -__LINE__;
    }

    int ret = update_user_balance(false, user_id, asset, business, business_id, change, detail);
    mpd_del(change);

    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}
*/

static int load_update_balance_v2(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // change
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    mpd_t *change = decimal(json_string_value(json_array_get(params, 1)), PREC_DEFAULT);
    if (change == NULL)
        return -__LINE__;

    // comment
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *comment = json_string_value(json_array_get(params, 2));

    int ret = update_user_balance_v2(false, sid, change, comment);
    mpd_del(change);

    if (ret < 0) {
        return -__LINE__;
    }

    return 0;
}

static int load_open_order2(json_t *params)
{
    // 跟开仓时候比多了create_time参数
    if (json_array_size(params) != 11)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return -__LINE__;

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return -__LINE__;

    // price
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // lot
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    mpd_t *lot = decimal(json_string_value(json_array_get(params, 5)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // tp
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    mpd_t *tp = mpd_new(&mpd_ctx);
    double dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    }
    if (tp == NULL)
        goto invalid_argument;

    // sl
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    mpd_t *sl = mpd_new(&mpd_ctx);
    dd = atof(json_string_value(json_array_get(params, 7)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 7)), PREC_PRICE);
    }
    if (sl == NULL)
        goto invalid_argument;

    // comment
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 8));

    // margin price
    if (!json_is_string(json_array_get(params, 9)))
        goto invalid_argument;
    mpd_t *margin_price = decimal(json_string_value(json_array_get(params, 9)), PREC_PRICE);
    if (margin_price == NULL || mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // create_time
    if (!json_is_real(json_array_get(params, 10)))
        goto invalid_argument;
    double create_time = json_real_value(json_array_get(params, 10));

    // fee
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);

    // swap
    mpd_t *swap = mpd_new(&mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

    int ret = market_open(false, NULL, market, get_symbol(symbol), sid, leverage, side, price, lot, tp, sl, fee, swap, 0, comment, margin_price, create_time);

    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(margin_price);
    mpd_del(fee);
    mpd_del(swap);

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
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);
    if (margin_price)
        mpd_del(margin_price);

    return -__LINE__;
}

static int load_close_order2(json_t *params)
{
    // 跟平仓时候比多了finish_time参数
    if (json_array_size(params) != 7)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    // price
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 3)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // comment
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 4));

    // profit price
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    mpd_t *profit_price = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    if (profit_price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // finish_time
    if (!json_is_real(json_array_get(params, 6)))
        goto invalid_argument;
    double finish_time = json_real_value(json_array_get(params, 6));

    int ret = market_close(false, NULL, market, get_symbol(symbol), sid, order, price, comment, profit_price, finish_time);

    mpd_del(price);
    mpd_del(profit_price);

    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return -__LINE__;
}

static int load_open_order(json_t *params)
{
    // 比开仓多了 price, margin_price, create_time 参数
    if (json_array_size(params) != 12)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return -__LINE__;

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return -__LINE__;

    // lot
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    mpd_t *lot = decimal(json_string_value(json_array_get(params, 4)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // tp
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    mpd_t *tp = mpd_new(&mpd_ctx);
    double dd = atof(json_string_value(json_array_get(params, 5)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    }
    if (tp == NULL)
        goto invalid_argument;

    // sl
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    mpd_t *sl = mpd_new(&mpd_ctx);
    dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    }
    if (sl == NULL)
        goto invalid_argument;

    // external
    if (!json_is_integer(json_array_get(params, 7)))
        return -__LINE__;
    uint64_t external = json_integer_value(json_array_get(params, 7));

    // comment
    if (!json_is_string(json_array_get(params, 8)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 8));

    // price
    if (!json_is_string(json_array_get(params, 9)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 9)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // margin price
    if (!json_is_string(json_array_get(params, 10)))
        goto invalid_argument;
    mpd_t *margin_price = decimal(json_string_value(json_array_get(params, 10)), PREC_PRICE);
    if (margin_price == NULL || mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // create_time
    if (!json_is_real(json_array_get(params, 11)))
        goto invalid_argument;
    double create_time = json_real_value(json_array_get(params, 11));

    // fee
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);

    // swap
    mpd_t *swap = mpd_new(&mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

//    int ret = market_open(false, NULL, market, get_symbol(symbol), sid, leverage, side, price, lot, tp, sl, fee, swap, external, comment, margin_price, create_time);
    int ret = market_open_hedged(false, NULL, market, get_symbol(symbol), sid, leverage, side, price, lot, tp, sl, symbol_percentage(group, symbol), fee, swap, external, comment, margin_price, create_time);

    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(margin_price);
    mpd_del(fee);
    mpd_del(swap);

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
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);
    if (margin_price)
        mpd_del(margin_price);

    return -__LINE__;
}

static int load_close_order(json_t *params)
{
    // 比平仓多了 price, profit_price, finish_time 参数
    if (json_array_size(params) != 7)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 3));

    // price
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // profit price
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    mpd_t *profit_price = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    if (profit_price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // finish_time
    if (!json_is_real(json_array_get(params, 6)))
        goto invalid_argument;
    double finish_time = json_real_value(json_array_get(params, 6));

//    int ret = market_close(false, NULL, market, get_symbol(symbol), sid, order, price, comment, profit_price, finish_time);
    int ret = market_close_hedged(false, NULL, market, get_symbol(symbol), sid, order, price, comment, profit_price, finish_time);

    mpd_del(price);
    mpd_del(profit_price);

    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return -__LINE__;
}

static int load_limit_order(json_t *params)
{
    // 比开仓多了 create_time 参数
    if (json_array_size(params) != 12)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // group
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *group = json_string_value(json_array_get(params, 1));

    // get group leverage
    int leverage = group_leverage(group);
    if (leverage == 0)
        return -__LINE__;

    // symbol
    if (!json_is_string(json_array_get(params, 2)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 2));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // side
    if (!json_is_integer(json_array_get(params, 3)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 3));
    if (side != ORDER_SIDE_BUY && side != ORDER_SIDE_SELL)
        return -__LINE__;

    // lot
    if (!json_is_string(json_array_get(params, 4)))
        goto invalid_argument;
    mpd_t *lot = decimal(json_string_value(json_array_get(params, 4)), PREC_DEFAULT);
    if (lot == NULL || mpd_cmp(lot, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // price
    if (!json_is_string(json_array_get(params, 5)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // tp
    if (!json_is_string(json_array_get(params, 6)))
        goto invalid_argument;
    mpd_t *tp = mpd_new(&mpd_ctx);
    double dd = atof(json_string_value(json_array_get(params, 6)));
    if (dd == 0) {
        mpd_copy(tp, mpd_zero, &mpd_ctx);
    } else {
        tp = decimal(json_string_value(json_array_get(params, 6)), PREC_PRICE);
    }
    if (tp == NULL)
        goto invalid_argument;

    // sl
    if (!json_is_string(json_array_get(params, 7)))
        goto invalid_argument;
    mpd_t *sl = mpd_new(&mpd_ctx);
    dd = atof(json_string_value(json_array_get(params, 7)));
    if (dd == 0) {
        mpd_copy(sl, mpd_zero, &mpd_ctx);
    } else {
        sl = decimal(json_string_value(json_array_get(params, 7)), PREC_PRICE);
    }
    if (sl == NULL)
        goto invalid_argument;

    // expire_time
    if (!json_is_integer(json_array_get(params, 8)))
        return -__LINE__;
    uint64_t expire_time = json_integer_value(json_array_get(params, 8));

    // external
    if (!json_is_integer(json_array_get(params, 9)))
        return -__LINE__;
    uint64_t external = json_integer_value(json_array_get(params, 9));

    // comment
    if (!json_is_string(json_array_get(params, 10)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 10));

    // create_time
    if (!json_is_real(json_array_get(params, 11)))
        goto invalid_argument;
    double create_time = json_real_value(json_array_get(params, 11));

    // fee
    mpd_t *fee = mpd_new(&mpd_ctx);
    mpd_copy(fee, symbol_fee(group, symbol), &mpd_ctx);
    mpd_mul(fee, lot, fee, &mpd_ctx);

    // swap
    mpd_t *swap = mpd_new(&mpd_ctx);
    if (side == ORDER_SIDE_BUY) {
        mpd_copy(swap, symbol_swap_long(group, symbol), &mpd_ctx);
    } else {
        mpd_copy(swap, symbol_swap_short(group, symbol), &mpd_ctx);
    }

    int ret = market_put_limit(false, NULL, market, sid, leverage, side, price, lot, tp, sl, symbol_percentage(group, symbol), fee, swap, external, comment, create_time, expire_time);

    mpd_del(price);
    mpd_del(lot);
    mpd_del(tp);
    mpd_del(sl);
    mpd_del(fee);
    mpd_del(swap);

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
    if (fee)
        mpd_del(fee);
    if (swap)
        mpd_del(swap);

    return -__LINE__;
}

static int load_cancel_order(json_t *params)
{
    // 比撤单多了 finish_time 参数
    if (json_array_size(params) != 5)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_limit(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *comment = json_string_value(json_array_get(params, 3));

    // finish_time
    if (!json_is_real(json_array_get(params, 4)))
        return -__LINE__;
    double finish_time = json_real_value(json_array_get(params, 4));

    return market_cancel(false, NULL, market, order, comment, finish_time);
}

static int load_cancel_external_order(json_t *params)
{
    // 比撤单多了 finish_time 参数
    if (json_array_size(params) != 5)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t external = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_external_limit(market, sid, external);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    const char *comment = json_string_value(json_array_get(params, 3));

    // finish_time
    if (!json_is_real(json_array_get(params, 4)))
        return -__LINE__;
    double finish_time = json_real_value(json_array_get(params, 4));

    return market_cancel(false, NULL, market, order, comment, finish_time);
}

static int load_limit_open(json_t *params)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_limit(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    // price
    if (!json_is_string(json_array_get(params, 3)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 3)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // margin_price
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *margin_price = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    if (margin_price == NULL || mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // update_time
    if (!json_is_real(json_array_get(params, 5)))
        goto invalid_argument;
    double update_time = json_real_value(json_array_get(params, 5));

    int ret = limit_open(false, market, get_symbol(symbol), order, sid, price, order->fee, margin_price, update_time);

    mpd_del(price);
    mpd_del(margin_price);

    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (margin_price)
        mpd_del(margin_price);

    return -__LINE__;
}

static int load_close_external_order(json_t *params)
{
    // 比平仓多了 price, profit_price, finish_time 参数
    if (json_array_size(params) != 7)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t external = json_integer_value(json_array_get(params, 2));
    if (external == 0)
        return -__LINE__;

    order_t *order = market_get_external_order(market, sid, external);
    if (order == NULL) {
        return -__LINE__;
    }

    // comment
    if (!json_is_string(json_array_get(params, 3)))
        goto invalid_argument;
    const char *comment = json_string_value(json_array_get(params, 3));

    // price
    if (!json_is_string(json_array_get(params, 4)))
        return -__LINE__;
    mpd_t *price = decimal(json_string_value(json_array_get(params, 4)), PREC_PRICE);
    if (price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // profit price
    if (!json_is_string(json_array_get(params, 5)))
        goto invalid_argument;
    mpd_t *profit_price = decimal(json_string_value(json_array_get(params, 5)), PREC_PRICE);
    if (profit_price == NULL || mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto invalid_argument;

    // finish_time
    if (!json_is_real(json_array_get(params, 6)))
        goto invalid_argument;
    double finish_time = json_real_value(json_array_get(params, 6));

//    int ret = market_close(false, NULL, market, get_symbol(symbol), sid, order, price, comment, profit_price, finish_time);
    int ret = market_close_hedged(false, NULL, market, get_symbol(symbol), sid, order, price, comment, profit_price, finish_time);

    mpd_del(price);
    mpd_del(profit_price);

    return ret;

invalid_argument:
    if (price)
        mpd_del(price);
    if (profit_price)
        mpd_del(profit_price);

    return -__LINE__;
}

static int load_update_order(json_t *params)
{
    if (json_array_size(params) != 5)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }
    if (order->sid != sid) {
        return -__LINE__;
    }

    int side = order->side;
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);

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

    int ret = market_update(false, NULL, market, order, tp, sl);

    mpd_del(tp);
    mpd_del(sl);

    return ret;

invalid_argument:
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);

    return -__LINE__;
}

static int load_update_external_order(json_t *params)
{
    if (json_array_size(params) != 5)
        return -__LINE__;

    // sid
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint64_t sid = json_integer_value(json_array_get(params, 0));

    // symbol
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *symbol = json_string_value(json_array_get(params, 1));

    market_t *market = get_market(symbol);
    if (market == NULL)
        return -__LINE__;

    // external
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t external = json_integer_value(json_array_get(params, 2));
    if (external == 0)
        return -__LINE__;

    order_t *order = market_get_external_order(market, sid, external);
    if (order == NULL) {
        return -__LINE__;
    }

    int side = order->side;
    mpd_t *tp = mpd_new(&mpd_ctx);
    mpd_t *sl = mpd_new(&mpd_ctx);

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

    int ret = market_update(false, NULL, market, order, tp, sl);

    mpd_del(tp);
    mpd_del(sl);

    return ret;

invalid_argument:
    if (tp)
        mpd_del(tp);
    if (sl)
        mpd_del(sl);

    return -__LINE__;
}

/*
static int load_limit_order(json_t *params)
{
    if (json_array_size(params) != 8)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount = NULL;
    mpd_t *price  = NULL;
    mpd_t *taker_fee = NULL;
    mpd_t *maker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // price 
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    price = decimal(json_string_value(json_array_get(params, 4)), market->money_prec);
    if (price == NULL) 
        goto error;
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 5)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // maker fee
    if (!json_is_string(json_array_get(params, 6)))
        goto error;
    maker_fee = decimal(json_string_value(json_array_get(params, 6)), market->fee_prec);
    if (maker_fee == NULL)
        goto error;
    if (mpd_cmp(maker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(maker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 7)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 7));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    int ret = market_put_limit_order(false, NULL, market, user_id, side, amount, price, taker_fee, maker_fee, source);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(taker_fee);
    mpd_del(maker_fee);

    return ret;

error:
    if (amount)
        mpd_del(amount);
    if (price)
        mpd_del(price);
    if (taker_fee)
        mpd_del(taker_fee);
    if (maker_fee)
        mpd_del(maker_fee);

    return -__LINE__;
}

static int load_market_order(json_t *params)
{
    if (json_array_size(params) != 6)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // side
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint32_t side = json_integer_value(json_array_get(params, 2));
    if (side != MARKET_ORDER_SIDE_ASK && side != MARKET_ORDER_SIDE_BID)
        return -__LINE__;

    mpd_t *amount = NULL;
    mpd_t *taker_fee = NULL;

    // amount
    if (!json_is_string(json_array_get(params, 3)))
        goto error;
    amount = decimal(json_string_value(json_array_get(params, 3)), market->stock_prec);
    if (amount == NULL)
        goto error;
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) <= 0)
        goto error;

    // taker fee
    if (!json_is_string(json_array_get(params, 4)))
        goto error;
    taker_fee = decimal(json_string_value(json_array_get(params, 4)), market->fee_prec);
    if (taker_fee == NULL)
        goto error;
    if (mpd_cmp(taker_fee, mpd_zero, &mpd_ctx) < 0 || mpd_cmp(taker_fee, mpd_one, &mpd_ctx) >= 0)
        goto error;

    // source
    if (!json_is_string(json_array_get(params, 5)))
        goto error;
    const char *source = json_string_value(json_array_get(params, 5));
    if (strlen(source) > SOURCE_MAX_LEN)
        goto error;

    int ret = market_put_market_order(false, NULL, market, user_id, side, amount, taker_fee, source);

    mpd_del(amount);
    mpd_del(taker_fee);

    return ret;

error:
    if (amount)
        mpd_del(amount);
    if (taker_fee)
        mpd_del(taker_fee);

    return -__LINE__;
}

static int load_cancel_order(json_t *params)
{
    if (json_array_size(params) != 3)
        return -__LINE__;

    // user_id
    if (!json_is_integer(json_array_get(params, 0)))
        return -__LINE__;
    uint32_t user_id = json_integer_value(json_array_get(params, 0));

    // market
    if (!json_is_string(json_array_get(params, 1)))
        return -__LINE__;
    const char *market_name = json_string_value(json_array_get(params, 1));
    market_t *market = get_market(market_name);
    if (market == NULL)
        return 0;

    // order_id
    if (!json_is_integer(json_array_get(params, 2)))
        return -__LINE__;
    uint64_t order_id = json_integer_value(json_array_get(params, 2));

    order_t *order = market_get_order(market, order_id);
    if (order == NULL) {
        return -__LINE__;
    }

    int ret = market_cancel_order(false, NULL, market, order);
    if (ret < 0) {
        log_error("market_cancel_order id: %"PRIu64", user id: %u, market: %s", order_id, user_id, market_name);
        return -__LINE__;
    }

    return 0;
}
*/

static int load_oper(json_t *detail)
{
    const char *method = json_string_value(json_object_get(detail, "method"));
    if (method == NULL)
        return -__LINE__;
    json_t *params = json_object_get(detail, "params");
    if (params == NULL || !json_is_array(params))
        return -__LINE__;

    int ret = 0;
    if (strcmp(method, "update_balance") == 0) {
        ret = load_update_balance_v2(params);
    } else if (strcmp(method, "open_order") == 0) {
        ret = load_open_order(params);
    } else if (strcmp(method, "close_order") == 0) {
        ret = load_close_order(params);
    } else if (strcmp(method, "open_order2") == 0) {
        ret = load_open_order2(params);
    } else if (strcmp(method, "close_order2") == 0) {
        ret = load_close_order2(params);
    } else if (strcmp(method, "tpsl_order") == 0) {
        ret = load_close_order(params);
    } else if (strcmp(method, "stop_out_order") == 0) {
        ret = load_close_order(params);
    } else if (strcmp(method, "update_order") == 0) {
        ret = load_update_order(params);
    } else if (strcmp(method, "limit_order") == 0) {
        ret = load_limit_order(params);
    } else if (strcmp(method, "cancel_order") == 0) {
        ret = load_cancel_order(params);
    } else if (strcmp(method, "limit_open") == 0) {
        ret = load_limit_open(params);
    } else if (strcmp(method, "close_external_order") == 0) {
        ret = load_close_external_order(params);
    } else if (strcmp(method, "update_external_order") == 0) {
        ret = load_update_external_order(params);
    } else if (strcmp(method, "cancel_external_order") == 0) {
        ret = load_cancel_external_order(params);
/*
    } else if (strcmp(method, "limit_order") == 0) {
        ret = load_limit_order(params);
    } else if (strcmp(method, "market_order") == 0) {
        ret = load_market_order(params);
    } else if (strcmp(method, "cancel_order") == 0) {
        ret = load_cancel_order(params);
*/
    } else {
        return -__LINE__;
    }

    return ret;
}

int load_operlog(MYSQL *conn, const char *table, uint64_t *start_id)
{
    size_t query_limit = 1000;
    uint64_t last_id = *start_id;
    while (true) {
        sds sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `id`, `detail` from `%s` WHERE `id` > %"PRIu64" ORDER BY `id` LIMIT %zu", table, last_id, query_limit);
        log_trace("exec sql: %s", sql);
        int ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(sql);
            return -__LINE__;
        }
        sdsfree(sql);

        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (size_t i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            uint64_t id = strtoull(row[0], NULL, 0);
            if (id != last_id + 1) {
                log_error("invalid id: %"PRIu64", last id: %"PRIu64"", id, last_id);
                return -__LINE__;
            }
            last_id = id;
            json_t *detail = json_loadb(row[1], strlen(row[1]), 0, NULL);
            if (detail == NULL) {
                log_error("invalid detail data: %s", row[1]);
                mysql_free_result(result);
                return -__LINE__;
            }
            ret = load_oper(detail);
            if (ret < 0) {
                json_decref(detail);
                log_error("load_oper: %"PRIu64":%s fail: %d", id, row[1], ret);
                mysql_free_result(result);
                return -__LINE__;
            }
            json_decref(detail);
        }
        mysql_free_result(result);

        if (num_rows < query_limit)
            break;
    }

    *start_id = last_id;
    return 0;
}

