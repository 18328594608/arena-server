# include "me_config.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_history.h"
# include "me_message.h"

uint64_t order_id_start;
uint64_t deals_id_start;
skiplist_t *expire_orders;

struct dict_user_key {
    uint32_t    user_id;
};

struct dict_sid_key {
    uint64_t    sid;
};

struct dict_order_key {
    uint64_t    order_id;
};

static uint32_t dict_user_hash_function(const void *key)
{
    const struct dict_user_key *obj = key;
    return obj->user_id;
}

static int dict_user_key_compare(const void *key1, const void *key2)
{
    const struct dict_user_key *obj1 = key1;
    const struct dict_user_key *obj2 = key2;
    if (obj1->user_id == obj2->user_id) {
        return 0;
    }
    return 1;
}

static void *dict_user_key_dup(const void *key)
{
    struct dict_user_key *obj = malloc(sizeof(struct dict_user_key));
    memcpy(obj, key, sizeof(struct dict_user_key));
    return obj;
}

static void dict_user_key_free(void *key)
{
    free(key);
}

static void dict_user_val_free(void *key)
{
    skiplist_release(key);
}

static uint32_t dict_sid_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_sid_key));
}

static int dict_sid_key_compare(const void *key1, const void *key2)
{
    const struct dict_sid_key *obj1 = key1;
    const struct dict_sid_key *obj2 = key2;
    if (obj1->sid == obj2->sid) {
        return 0;
    }
    return 1;
}

static void *dict_sid_key_dup(const void *key)
{
    struct dict_sid_key *obj = malloc(sizeof(struct dict_sid_key));
    memcpy(obj, key, sizeof(struct dict_sid_key));
    return obj;
}

static void dict_sid_key_free(void *key)
{
    free(key);
}

static void dict_sid_val_free(void *key)
{
    skiplist_release(key);
}

static void dict_margin_val_free(void *val)
{
    mpd_del(val);
}

static uint32_t dict_order_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct dict_order_key));
}

static int dict_order_key_compare(const void *key1, const void *key2)
{
    const struct dict_order_key *obj1 = key1;
    const struct dict_order_key *obj2 = key2;
    if (obj1->order_id == obj2->order_id) {
        return 0;
    }
    return 1;
}

static void *dict_order_key_dup(const void *key)
{
    struct dict_order_key *obj = malloc(sizeof(struct dict_order_key));
    memcpy(obj, key, sizeof(struct dict_order_key));
    return obj;
}

static void dict_order_key_free(void *key)
{
    free(key);
}

static int order_match_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }
    if (order1->type != order2->type) {
        return 1;
    }

    int cmp;
    if (order1->side == ORDER_SIDE_SELL) {
        cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    } else {
        cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
    }
    if (cmp != 0) {
        return cmp;
    }

    return order1->id > order2->id ? 1 : -1;
}

static int order_id_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;
    if (order1->id == order2->id) {
        return 0;
    }

    return order1->id > order2->id ? -1 : 1;
}

// 止盈止损订单排序规则，相同值按订单号排序
// buy tp / sell sl 从低到高
// buy sl / sell tp 从高到低
static int order_buy_tp_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order1->tp, order2->tp, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

static int order_sell_sl_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order1->sl, order2->sl, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

static int order_buy_sl_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order2->sl, order1->sl, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

static int order_sell_tp_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order2->tp, order1->tp, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

// 挂单排序规则，相同值按订单号排序
// buy limit 从高到低
// sell limit 从低到高
static int order_buy_limit_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order2->price, order1->price, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

static int order_sell_limit_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order1->price, order2->price, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

static void order_free(order_t *order)
{
/*
    mpd_del(order->price);
    mpd_del(order->amount);
    mpd_del(order->taker_fee);
    mpd_del(order->maker_fee);
    mpd_del(order->left);
    mpd_del(order->freeze);
    mpd_del(order->deal_stock);
    mpd_del(order->deal_money);
    mpd_del(order->deal_fee);
    free(order->market);
    free(order->source);
    free(order);
*/
}

json_t *get_order_info(order_t *order)
{
    json_t *info = json_object();
/*
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "market", json_string(order->market));
    json_object_set_new(info, "source", json_string(order->source));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "user", json_integer(order->user_id));
    json_object_set_new(info, "ctime", json_real(order->create_time));
    json_object_set_new(info, "mtime", json_real(order->update_time));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "amount", order->amount);
    json_object_set_new_mpd(info, "taker_fee", order->taker_fee);
    json_object_set_new_mpd(info, "maker_fee", order->maker_fee);
    json_object_set_new_mpd(info, "left", order->left);
    json_object_set_new_mpd(info, "deal_stock", order->deal_stock);
    json_object_set_new_mpd(info, "deal_money", order->deal_money);
    json_object_set_new_mpd(info, "deal_fee", order->deal_fee);
*/
    return info;
}

static int order_put(market_t *m, order_t *order)
{
/*
    if (order->type != MARKET_ORDER_TYPE_LIMIT)
        return -__LINE__;

    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->users, &user_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == MARKET_ORDER_SIDE_ASK) {
        if (skiplist_insert(m->asks, order) == NULL)
            return -__LINE__;
        mpd_copy(order->freeze, order->left, &mpd_ctx);
        if (balance_freeze(order->user_id, m->stock, order->left) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->bids, order) == NULL)
            return -__LINE__;
        mpd_t *result = mpd_new(&mpd_ctx);
        mpd_mul(result, order->price, order->left, &mpd_ctx);
        mpd_copy(order->freeze, result, &mpd_ctx);
        if (balance_freeze(order->user_id, m->money, result) == NULL) {
            mpd_del(result);
            return -__LINE__;
        }
        mpd_del(result);
    }
*/
    return 0;
}

static int order_finish(bool real, market_t *m, order_t *order)
{
/*
    if (order->side == MARKET_ORDER_SIDE_ASK) {
        skiplist_node *node = skiplist_find(m->asks, order);
        if (node) {
            skiplist_delete(m->asks, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->stock, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    } else {
        skiplist_node *node = skiplist_find(m->bids, order);
        if (node) {
            skiplist_delete(m->bids, node);
        }
        if (mpd_cmp(order->freeze, mpd_zero, &mpd_ctx) > 0) {
            if (balance_unfreeze(order->user_id, m->money, order->freeze) == NULL) {
                return -__LINE__;
            }
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_user_key user_key = { .user_id = order->user_id };
    dict_entry *entry = dict_find(m->users, &user_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

    if (real) {
        if (mpd_cmp(order->deal_stock, mpd_zero, &mpd_ctx) > 0) {
            int ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
        }
    }

    order_free(order);
*/
    return 0;
}

/*
market_t *market_create(struct market *conf)
{
    if (!asset_exist(conf->stock) || !asset_exist(conf->money))
        return NULL;

    if (conf->stock_prec + conf->money_prec > asset_prec(conf->money))
        return NULL;
    if (conf->stock_prec + conf->fee_prec > asset_prec(conf->stock))
        return NULL;
    if (conf->money_prec + conf->fee_prec > asset_prec(conf->money))
        return NULL;

    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);
    m->stock            = strdup(conf->stock);
    m->money            = strdup(conf->money);
    m->stock_prec       = conf->stock_prec;
    m->money_prec       = conf->money_prec;
    m->fee_prec         = conf->fee_prec;
    m->min_amount       = mpd_qncopy(conf->min_amount);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_user_hash_function;
    dt.key_compare      = dict_user_key_compare;
    dt.key_dup          = dict_user_key_dup;
    dt.key_destructor   = dict_user_key_free;
    dt.val_destructor   = dict_user_val_free;

    m->users = dict_create(&dt, 1024);
    if (m->users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare          = order_match_compare;

    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    return m;
}
*/

market_t *market_create_v2(struct symbol *conf)
{
    market_t *m = malloc(sizeof(market_t));
    memset(m, 0, sizeof(market_t));
    m->name             = strdup(conf->name);

    dict_types dt;
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_sid_hash_function;
    dt.key_compare      = dict_sid_key_compare;
    dt.key_dup          = dict_sid_key_dup;
    dt.key_destructor   = dict_sid_key_free;
    dt.val_destructor   = dict_sid_val_free;

    m->users = dict_create(&dt, 1024);
    if (m->users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->orders = dict_create(&dt, 1024);
    if (m->orders == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_sid_hash_function;
    dt.key_compare      = dict_sid_key_compare;
    dt.key_dup          = dict_sid_key_dup;
    dt.key_destructor   = dict_sid_key_free;
    dt.val_destructor   = dict_margin_val_free;

    m->margins = dict_create(&dt, 1024);
    if (m->margins == NULL)
        return NULL;

    skiplist_type lt;
    memset(&lt, 0, sizeof(lt));
    lt.compare = order_match_compare;

    m->buys = skiplist_create(&lt);
    m->sells = skiplist_create(&lt);
    if (m->buys == NULL || m->sells == NULL)
        return NULL;

    m->asks = skiplist_create(&lt);
    m->bids = skiplist_create(&lt);
    if (m->asks == NULL || m->bids == NULL)
        return NULL;

    // limit
    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_sid_hash_function;
    dt.key_compare      = dict_sid_key_compare;
    dt.key_dup          = dict_sid_key_dup;
    dt.key_destructor   = dict_sid_key_free;
    dt.val_destructor   = dict_sid_val_free;

    m->limit_users = dict_create(&dt, 1024);
    if (m->limit_users == NULL)
        return NULL;

    memset(&dt, 0, sizeof(dt));
    dt.hash_function    = dict_order_hash_function;
    dt.key_compare      = dict_order_key_compare;
    dt.key_dup          = dict_order_key_dup;
    dt.key_destructor   = dict_order_key_free;

    m->limit_orders = dict_create(&dt, 1024);
    if (m->limit_orders == NULL)
        return NULL;

    skiplist_type bl;
    memset(&bl, 0, sizeof(bl));
    bl.compare = order_buy_limit_compare;

    skiplist_type sl;
    memset(&sl, 0, sizeof(sl));
    sl.compare = order_sell_limit_compare;

    m->limit_buys = skiplist_create(&bl);
    m->limit_sells = skiplist_create(&sl);
    if (m->limit_buys == NULL || m->limit_sells == NULL)
        return NULL;

    // tp sl
    skiplist_type bt;
    memset(&bt, 0, sizeof(bt));
    bt.compare = order_buy_tp_compare;

    skiplist_type ss;
    memset(&ss, 0, sizeof(ss));
    ss.compare = order_sell_sl_compare;

    skiplist_type bs;
    memset(&bs, 0, sizeof(bs));
    bs.compare = order_buy_sl_compare;

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.compare = order_sell_tp_compare;

    m->tp_buys = skiplist_create(&bt);
    m->sl_buys = skiplist_create(&bs);
    m->tp_sells = skiplist_create(&st);
    m->sl_sells = skiplist_create(&ss);
    if (m->tp_buys == NULL || m->sl_buys == NULL  || m->tp_sells == NULL  || m->sl_sells == NULL)
        return NULL;

    return m;
}

/*
static int append_balance_trade_add(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", change, detail_str);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_sub(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}

static int append_balance_trade_fee(order_t *order, const char *asset, mpd_t *change, mpd_t *price, mpd_t *amount, mpd_t *fee_rate)
{
    json_t *detail = json_object();
    json_object_set_new(detail, "m", json_string(order->market));
    json_object_set_new(detail, "i", json_integer(order->id));
    json_object_set_new_mpd(detail, "p", price);
    json_object_set_new_mpd(detail, "a", amount);
    json_object_set_new_mpd(detail, "f", fee_rate);
    char *detail_str = json_dumps(detail, JSON_SORT_KEYS);
    mpd_t *real_change = mpd_new(&mpd_ctx);
    mpd_copy_negate(real_change, change, &mpd_ctx);
    int ret = append_user_balance_history(order->update_time, order->user_id, asset, "trade", real_change, detail_str);
    mpd_del(real_change);
    free(detail_str);
    json_decref(detail);
    return ret;
}
*/

static int execute_limit_ask_order(bool real, market_t *m, order_t *taker)
{
/*
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) > 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);
*/
    return 0;
}

static int execute_limit_bid_order(bool real, market_t *m, order_t *taker)
{
/*
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        if (mpd_cmp(taker->price, maker->price, &mpd_ctx) < 0) {
            break;
        }

        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);
*/
    return 0;
}

int market_put_limit_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *price, mpd_t *taker_fee, mpd_t *maker_fee, const char *source)
{
/*
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, amount, price, &mpd_ctx);
        if (!balance || mpd_cmp(balance, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -1;
        }
        mpd_del(require);
    }

    if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
        return -2;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, maker_fee, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_limit_ask_order(real, m, order);
    } else {
        ret = execute_limit_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (mpd_cmp(order->left, mpd_zero, &mpd_ctx) == 0) {
        if (real) {
            ret = append_order_history(order);
            if (ret < 0) {
                log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
            }
            push_order_message(ORDER_EVENT_FINISH, order, m);
            *result = get_order_info(order);
        }
        order_free(order);
    } else {
        if (real) {
            push_order_message(ORDER_EVENT_PUT, order, m);
            *result = get_order_info(order);
        }
        ret = order_put(m, order);
        if (ret < 0) {
            log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }
*/
    return 0;
}

static int execute_market_ask_order(bool real, market_t *m, order_t *taker)
{
/*
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);
        if (mpd_cmp(taker->left, maker->left, &mpd_ctx) < 0) {
            mpd_copy(amount, taker->left, &mpd_ctx);
        } else {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, taker->taker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, maker->maker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, taker, MARKET_ROLE_TAKER, maker, MARKET_ROLE_MAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, taker, maker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_ASK, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, amount, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(taker, m->stock, amount, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(taker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(taker, m->money, ask_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, deal, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->money, deal);
        if (real) {
            append_balance_trade_sub(maker, m->money, deal, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(maker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(maker, m->stock, bid_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);
*/
    return 0;
}

static int execute_market_bid_order(bool real, market_t *m, order_t *taker)
{
/*
    mpd_t *price    = mpd_new(&mpd_ctx);
    mpd_t *amount   = mpd_new(&mpd_ctx);
    mpd_t *deal     = mpd_new(&mpd_ctx);
    mpd_t *ask_fee  = mpd_new(&mpd_ctx);
    mpd_t *bid_fee  = mpd_new(&mpd_ctx);
    mpd_t *result   = mpd_new(&mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        if (mpd_cmp(taker->left, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        order_t *maker = node->value;
        mpd_copy(price, maker->price, &mpd_ctx);

        mpd_div(amount, taker->left, price, &mpd_ctx);
        mpd_rescale(amount, amount, -m->stock_prec, &mpd_ctx);
        while (true) {
            mpd_mul(result, amount, price, &mpd_ctx);
            if (mpd_cmp(result, taker->left, &mpd_ctx) > 0) {
                mpd_set_i32(result, -m->stock_prec, &mpd_ctx);
                mpd_pow(result, mpd_ten, result, &mpd_ctx);
                mpd_sub(amount, amount, result, &mpd_ctx);
            } else {
                break;
            }
        }

        if (mpd_cmp(amount, maker->left, &mpd_ctx) > 0) {
            mpd_copy(amount, maker->left, &mpd_ctx);
        }
        if (mpd_cmp(amount, mpd_zero, &mpd_ctx) == 0) {
            break;
        }

        mpd_mul(deal, price, amount, &mpd_ctx);
        mpd_mul(ask_fee, deal, maker->maker_fee, &mpd_ctx);
        mpd_mul(bid_fee, amount, taker->taker_fee, &mpd_ctx);

        taker->update_time = maker->update_time = current_timestamp();
        uint64_t deal_id = ++deals_id_start;
        if (real) {
            append_order_deal_history(taker->update_time, deal_id, maker, MARKET_ROLE_MAKER, taker, MARKET_ROLE_TAKER, price, amount, deal, ask_fee, bid_fee);
            push_deal_message(taker->update_time, m->name, maker, taker, price, amount, ask_fee, bid_fee, MARKET_ORDER_SIDE_BID, deal_id, m->stock, m->money);
        }

        mpd_sub(taker->left, taker->left, deal, &mpd_ctx);
        mpd_add(taker->deal_stock, taker->deal_stock, amount, &mpd_ctx);
        mpd_add(taker->deal_money, taker->deal_money, deal, &mpd_ctx);
        mpd_add(taker->deal_fee, taker->deal_fee, bid_fee, &mpd_ctx);

        balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_sub(taker, m->money, deal, price, amount);
        }
        balance_add(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, amount);
        if (real) {
            append_balance_trade_add(taker, m->stock, amount, price, amount);
        }
        if (mpd_cmp(bid_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(taker->user_id, BALANCE_TYPE_AVAILABLE, m->stock, bid_fee);
            if (real) {
                append_balance_trade_fee(taker, m->stock, bid_fee, price, amount, taker->taker_fee);
            }
        }

        mpd_sub(maker->left, maker->left, amount, &mpd_ctx);
        mpd_sub(maker->freeze, maker->freeze, amount, &mpd_ctx);
        mpd_add(maker->deal_stock, maker->deal_stock, amount, &mpd_ctx);
        mpd_add(maker->deal_money, maker->deal_money, deal, &mpd_ctx);
        mpd_add(maker->deal_fee, maker->deal_fee, ask_fee, &mpd_ctx);

        balance_sub(maker->user_id, BALANCE_TYPE_FREEZE, m->stock, amount);
        if (real) {
            append_balance_trade_sub(maker, m->stock, amount, price, amount);
        }
        balance_add(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, deal);
        if (real) {
            append_balance_trade_add(maker, m->money, deal, price, amount);
        }
        if (mpd_cmp(ask_fee, mpd_zero, &mpd_ctx) > 0) {
            balance_sub(maker->user_id, BALANCE_TYPE_AVAILABLE, m->money, ask_fee);
            if (real) {
                append_balance_trade_fee(maker, m->money, ask_fee, price, amount, maker->maker_fee);
            }
        }

        if (mpd_cmp(maker->left, mpd_zero, &mpd_ctx) == 0) {
            if (real) {
                push_order_message(ORDER_EVENT_FINISH, maker, m);
            }
            order_finish(real, m, maker);
        } else {
            if (real) {
                push_order_message(ORDER_EVENT_UPDATE, maker, m);
            }
        }
    }
    skiplist_release_iterator(iter);

    mpd_del(amount);
    mpd_del(price);
    mpd_del(deal);
    mpd_del(ask_fee);
    mpd_del(bid_fee);
    mpd_del(result);
*/
    return 0;
}

int market_put_market_order(bool real, json_t **result, market_t *m, uint32_t user_id, uint32_t side, mpd_t *amount, mpd_t *taker_fee, const char *source)
{
/*
    if (side == MARKET_ORDER_SIDE_ASK) {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->stock);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->bids);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        if (mpd_cmp(amount, m->min_amount, &mpd_ctx) < 0) {
            return -2;
        }
    } else {
        mpd_t *balance = balance_get(user_id, BALANCE_TYPE_AVAILABLE, m->money);
        if (!balance || mpd_cmp(balance, amount, &mpd_ctx) < 0) {
            return -1;
        }

        skiplist_iter *iter = skiplist_get_iterator(m->asks);
        skiplist_node *node = skiplist_next(iter);
        if (node == NULL) {
            skiplist_release_iterator(iter);
            return -3;
        }
        skiplist_release_iterator(iter);

        order_t *order = node->value;
        mpd_t *require = mpd_new(&mpd_ctx);
        mpd_mul(require, order->price, m->min_amount, &mpd_ctx);
        if (mpd_cmp(amount, require, &mpd_ctx) < 0) {
            mpd_del(require);
            return -2;
        }
        mpd_del(require);
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = current_timestamp();
    order->update_time  = order->create_time;
    order->market       = strdup(m->name);
    order->source       = strdup(source);
    order->user_id      = user_id;
    order->price        = mpd_new(&mpd_ctx);
    order->amount       = mpd_new(&mpd_ctx);
    order->taker_fee    = mpd_new(&mpd_ctx);
    order->maker_fee    = mpd_new(&mpd_ctx);
    order->left         = mpd_new(&mpd_ctx);
    order->freeze       = mpd_new(&mpd_ctx);
    order->deal_stock   = mpd_new(&mpd_ctx);
    order->deal_money   = mpd_new(&mpd_ctx);
    order->deal_fee     = mpd_new(&mpd_ctx);

    mpd_copy(order->price, mpd_zero, &mpd_ctx);
    mpd_copy(order->amount, amount, &mpd_ctx);
    mpd_copy(order->taker_fee, taker_fee, &mpd_ctx);
    mpd_copy(order->maker_fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->left, amount, &mpd_ctx);
    mpd_copy(order->freeze, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_stock, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_money, mpd_zero, &mpd_ctx);
    mpd_copy(order->deal_fee, mpd_zero, &mpd_ctx);

    int ret;
    if (side == MARKET_ORDER_SIDE_ASK) {
        ret = execute_market_ask_order(real, m, order);
    } else {
        ret = execute_market_bid_order(real, m, order);
    }
    if (ret < 0) {
        log_error("execute order: %"PRIu64" fail: %d", order->id, ret);
        order_free(order);
        return -__LINE__;
    }

    if (real) {
        int ret = append_order_history(order);
        if (ret < 0) {
            log_fatal("append_order_history fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }

    order_free(order);
*/
    return 0;
}

int market_cancel_order(bool real, json_t **result, market_t *m, order_t *order)
{
    if (real) {
        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info(order);
    }
    order_finish(real, m, order);
    return 0;
}

int market_put_order(market_t *m, order_t *order)
{
    return order_put(m, order);
}

order_t *market_get_order(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *market_get_order_list(market_t *m, uint32_t user_id)
{
    struct dict_user_key key = { .user_id = user_id };
    dict_entry *entry = dict_find(m->users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

skiplist_t *market_get_order_list_v2(market_t *m, uint64_t sid)
{
    struct dict_sid_key key = { .sid = sid };
    dict_entry *entry = dict_find(m->users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

order_t *market_get_external_order(market_t *m, uint64_t sid, uint64_t external)
{
    skiplist_t *list = market_get_order_list_v2(m, sid);
    if (list == NULL) {
        return NULL;
    }

    order_t *order = NULL;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *o = node->value;
        if (o->external == external) {
            order = o;
            break;
        }
    }
    skiplist_release_iterator(iter);
    return order;
}

skiplist_t *market_get_limit_list(market_t *m, uint64_t sid)
{
    struct dict_sid_key key = { .sid = sid };
    dict_entry *entry = dict_find(m->limit_users, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

order_t *market_get_external_limit(market_t *m, uint64_t sid, uint64_t external)
{
    skiplist_t *list = market_get_limit_list(m, sid);
    if (list == NULL) {
        return NULL;
    }

    order_t *order = NULL;
    skiplist_iter *iter = skiplist_get_iterator(list);
    skiplist_node *node;
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *o = node->value;
        if (o->external == external) {
            order = o;
            break;
        }
    }
    skiplist_release_iterator(iter);
    return order;
}

int market_get_status(market_t *m, size_t *ask_count, mpd_t *ask_amount, size_t *bid_count, mpd_t *bid_amount)
{
/*
    *ask_count = m->asks->len;
    *bid_count = m->bids->len;
    mpd_copy(ask_amount, mpd_zero, &mpd_ctx);
    mpd_copy(bid_amount, mpd_zero, &mpd_ctx);

    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(m->asks);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(ask_amount, ask_amount, order->left, &mpd_ctx);
    }
    skiplist_release_iterator(iter);

    iter = skiplist_get_iterator(m->bids);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        mpd_add(bid_amount, bid_amount, order->left, &mpd_ctx);
    }
*/
    return 0;
}

sds market_status(sds reply)
{
    reply = sdscatprintf(reply, "order last ID: %"PRIu64"\n", order_id_start);
    reply = sdscatprintf(reply, "deals last ID: %"PRIu64"\n", deals_id_start);
    return reply;
}

static int order_put_v2(market_t *m, order_t *order)
{
    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_sid_key sid_key = { .sid = order->sid };
    dict_entry *entry = dict_find(m->users, &sid_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->users, &sid_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == ORDER_SIDE_BUY) {
        if (skiplist_insert(m->buys, order) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->sells, order) == NULL)
            return -__LINE__;
    }

    if (mpd_cmp(order->tp, mpd_zero, &mpd_ctx) > 0) {
        if (order->side == ORDER_SIDE_BUY) {
            if (skiplist_insert(m->tp_buys, order) == NULL)
                return -__LINE__;
        } else {
            if (skiplist_insert(m->tp_sells, order) == NULL)
                return -__LINE__;
        }
    }
    if (mpd_cmp(order->sl, mpd_zero, &mpd_ctx) > 0) {
        if (order->side == ORDER_SIDE_BUY) {
            if (skiplist_insert(m->sl_buys, order) == NULL)
                return -__LINE__;
        } else {
            if (skiplist_insert(m->sl_sells, order) == NULL)
                return -__LINE__;
        }
    }

    return 0;
}

json_t *get_order_info_v2(order_t *order)
{
    json_t *info = json_object();
    json_object_set_new(info, "id", json_integer(order->id));
    json_object_set_new(info, "type", json_integer(order->type));
    json_object_set_new(info, "side", json_integer(order->side));
    json_object_set_new(info, "external", json_integer(order->external));
    json_object_set_new(info, "create_time", json_real(order->create_time));
    json_object_set_new(info, "update_time", json_real(order->update_time));
    json_object_set_new(info, "finish_time", json_real(order->finish_time));
    json_object_set_new(info, "expire_time", json_integer(order->expire_time));
    json_object_set_new(info, "sid", json_integer(order->sid));
    json_object_set_new(info, "symbol", json_string(order->symbol));

    json_object_set_new_mpd(info, "price", order->price);
    json_object_set_new_mpd(info, "lot", order->lot);
    json_object_set_new_mpd(info, "close_price", order->close_price);
    json_object_set_new_mpd(info, "margin", order->margin);
    json_object_set_new_mpd(info, "fee", order->fee);
    json_object_set_new_mpd(info, "swap", order->swap);
    json_object_set_new_mpd(info, "swaps", order->swaps);
    json_object_set_new_mpd(info, "profit", order->profit);
    json_object_set_new_mpd(info, "tp", order->tp);
    json_object_set_new_mpd(info, "sl", order->sl);

    json_object_set_new_mpd(info, "margin_price", order->margin_price);
    json_object_set_new_mpd(info, "profit_price", order->profit_price);

    json_object_set_new(info, "comment", json_string(order->comment));

    return info;
}

static void order_free_v2(order_t *order)
{
    mpd_del(order->price);
    mpd_del(order->close_price);
    mpd_del(order->lot);
    mpd_del(order->margin);
    mpd_del(order->fee);
    mpd_del(order->swap);
    mpd_del(order->swaps);
    mpd_del(order->profit);
    mpd_del(order->tp);
    mpd_del(order->sl);
    mpd_del(order->margin_price);
    mpd_del(order->profit_price);
    free(order->symbol);
    free(order->comment);
    free(order);
}

int market_put_position(market_t *m, order_t *order)
{
    return order_put_v2(m, order);
}

int market_open(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot,
                mpd_t *tp, mpd_t *sl, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, mpd_t *margin_price, double create_time)
{
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0) {
       return -3;
    }
    if (mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0) {
       return -4;
    }

    // 1.计算保证金, c = contract_size * percentage / 100
    // Forex = lots * contract_size / leverage * percentage / 100
    // CFD = lots * contract_size / leverage * percentage / 100 * market_price
    mpd_t *margin = mpd_new(&mpd_ctx);
    mpd_set_u32(margin, leverage, &mpd_ctx);
    mpd_div(margin, sym->c, margin, &mpd_ctx);
    mpd_mul(margin, margin, lot, &mpd_ctx);

    if (sym->margin_calc == MARGIN_CALC_FOREX) {
        if (sym->margin_type == MARGIN_TYPE_AU) {
            mpd_mul(margin, margin, price, &mpd_ctx);        // EURUSD
        } else if (sym->margin_type == MARGIN_TYPE_AC) {
            mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURGBP
        } else if (sym->margin_type == MARGIN_TYPE_BC) {
            mpd_div(margin, margin, margin_price, &mpd_ctx); // CADJPY
        }
    } else {
         if (strcmp(sym->name, "HSI") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDHKD
         } else if (strcmp(sym->name, "DAX") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURUSD
         } else if (strcmp(sym->name, "UK100") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // GBPUSD
         } else if (strcmp(sym->name, "JP225") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDJPY
         }
         mpd_mul(margin, margin, price, &mpd_ctx);
    }
    mpd_rescale(margin, margin, -2, &mpd_ctx);

    // 2.判断可用金是否足够
    mpd_t *free = balance_get_v2(sid, BALANCE_TYPE_FREE);
    if (free) {
        mpd_t *total = mpd_new(&mpd_ctx);
        mpd_copy(total, margin, &mpd_ctx);
        mpd_add(total, margin, fee, &mpd_ctx);
        if (mpd_cmp(free, total, &mpd_ctx) > 0) {
            balance_add_v2(sid, BALANCE_TYPE_MARGIN, margin);
            balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
            balance_sub_v2(sid, BALANCE_TYPE_FREE, total);
            mpd_del(total);
        } else {
log_info("## free = %s, margin = %s, total = %s", mpd_to_sci(free, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
            mpd_del(margin);
            mpd_del(total);
            return -2;
        }
    } else {
        mpd_del(margin);
        return -2;
    }

    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = create_time;
    order->update_time  = 0;
    order->finish_time  = 0;
    order->expire_time  = 0;
    order->sid          = sid;
    order->external     = external;
    order->symbol       = strdup(m->name);
    order->comment      = strdup(comment);

    order->price        = mpd_new(&mpd_ctx);
    order->lot          = mpd_new(&mpd_ctx);
    order->close_price  = mpd_new(&mpd_ctx);
    order->margin       = mpd_new(&mpd_ctx);
    order->fee          = mpd_new(&mpd_ctx);
    order->swap         = mpd_new(&mpd_ctx);
    order->swaps        = mpd_new(&mpd_ctx);
    order->profit       = mpd_new(&mpd_ctx);
    order->tp           = mpd_new(&mpd_ctx);
    order->sl           = mpd_new(&mpd_ctx);
    order->margin_price = mpd_new(&mpd_ctx);
    order->profit_price = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->lot, lot, &mpd_ctx);
    mpd_copy(order->fee, fee, &mpd_ctx);
    mpd_copy(order->swap, swap, &mpd_ctx);
    mpd_copy(order->swaps, mpd_zero, &mpd_ctx);
    mpd_copy(order->tp, tp, &mpd_ctx);
    mpd_copy(order->sl, sl, &mpd_ctx);
    mpd_copy(order->close_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->margin, margin, &mpd_ctx);
    mpd_copy(order->profit, mpd_zero, &mpd_ctx);
    mpd_copy(order->margin_price, margin_price, &mpd_ctx);
    mpd_copy(order->profit_price, mpd_one, &mpd_ctx);

    mpd_del(margin);

    int ret = order_put_v2(m, order);
    if (ret < 0) {
        log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        return ret;
    }
    if (real) {
        ret = append_position(order);
        if (ret < 0) {
            log_fatal("append_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
//        push_order_message(ORDER_EVENT_PUT, order, m);
        *result = get_order_info_v2(order);
    }

    return 0;
}

static int order_finish_v2(market_t *m, order_t *order)
{
    if (order->side == ORDER_SIDE_SELL) {
        skiplist_node *node = skiplist_find(m->sells, order);
        if (node) {
            skiplist_delete(m->sells, node);
        }

        if (mpd_cmp(order->tp, mpd_zero, &mpd_ctx) > 0) {
            skiplist_node *tp_node = skiplist_find(m->tp_sells, order);
            if (tp_node) {
                skiplist_delete(m->tp_sells, tp_node);
            }
        }
        if (mpd_cmp(order->sl, mpd_zero, &mpd_ctx) > 0) {
            skiplist_node *sl_node = skiplist_find(m->sl_sells, order);
            if (sl_node) {
                skiplist_delete(m->sl_sells, sl_node);
            }
        }
    } else {
        skiplist_node *node = skiplist_find(m->buys, order);
        if (node) {
            skiplist_delete(m->buys, node);
        }

        if (mpd_cmp(order->tp, mpd_zero, &mpd_ctx) > 0) {
            skiplist_node *tp_node = skiplist_find(m->tp_buys, order);
            if (tp_node) {
                skiplist_delete(m->tp_buys, tp_node);
            }
        }
        if (mpd_cmp(order->sl, mpd_zero, &mpd_ctx) > 0) {
            skiplist_node *sl_node = skiplist_find(m->sl_buys, order);
            if (sl_node) {
                skiplist_delete(m->sl_buys, sl_node);
            }
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_sid_key sid_key = { .sid = order->sid };
    dict_entry *entry = dict_find(m->users, &sid_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

    order_free_v2(order);
    return 0;
}

int market_close(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time)
{
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0) {
       return -3;
    }
    if (mpd_cmp(profit_price, mpd_zero, &mpd_ctx) <= 0) {
       return -5;
    }

    // 1.计算盈亏
    // Forex / CFD = (close_price - open_price) * contract_size * lots
    // Futures = (close_price - open_price) * tick_price / tick_size * lots
    mpd_t *profit = mpd_new(&mpd_ctx);
    if (order->side == ORDER_SIDE_BUY) {
        mpd_sub(profit, price, order->price, &mpd_ctx);
    } else {
        mpd_sub(profit, order->price, price, &mpd_ctx);
    }
    mpd_mul(profit, profit, order->lot, &mpd_ctx);

    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        mpd_mul(profit, profit, sym->contract_size, &mpd_ctx);
        if (sym->profit_type == PROFIT_TYPE_UB) {
            mpd_div(profit, profit, price, &mpd_ctx);        // USDJPY
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

    if (real) {
        // 2.update float
        balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);
    }

    // 3.update order
    mpd_copy(order->profit, profit, &mpd_ctx);
    mpd_copy(order->close_price, price, &mpd_ctx);
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    // 4.update equity,margin,margin_free
    balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
    balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        if (real) {
            balance_sub_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
            balance_sub_v2(sid, BALANCE_TYPE_FREE, abs_profit);
        } else {
            balance_sub_float(sid, BALANCE_TYPE_EQUITY, abs_profit);
            balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
        }
    }

    // 5.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        if (real)
            balance = balance_sub_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
        else
            balance = balance_sub_float(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL)
        return -__LINE__;

    int ret = 0;

    // append history
    if (real) {
        double now = current_timestamp();
        ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
        if (ret < 0) {
            log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    if (real) {
        ret = finish_position(order);
        if (ret < 0) {
            log_fatal("finish_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
//        push_order_message(ORDER_EVENT_FINISH, order, m);
        *result = get_order_info_v2(order);
    }

    order_finish_v2(m, order);
    return 0;
}

static int order_tpsl(market_t *m, order_t *order)
{
    if (order->side == ORDER_SIDE_SELL) {
        skiplist_node *node = skiplist_find(m->sells, order);
        if (node) {
            skiplist_delete(m->sells, node);
        }
    } else {
        skiplist_node *node = skiplist_find(m->buys, order);
        if (node) {
            skiplist_delete(m->buys, node);
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->orders, &order_key);

    struct dict_sid_key sid_key = { .sid = order->sid };
    dict_entry *entry = dict_find(m->users, &sid_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

//    order_free_v2(order);
    return 0;
}

int market_tpsl(bool real, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time)
{
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0) {
       return -3;
    }
    if (mpd_cmp(profit_price, mpd_zero, &mpd_ctx) <= 0) {
       return -5;
    }

    // 1.计算盈亏
    // Forex / CFD = (close_price - open_price) * contract_size * lots
    // Futures = (close_price - open_price) * tick_price / tick_size * lots
    mpd_t *profit = mpd_new(&mpd_ctx);
    if (order->side == ORDER_SIDE_BUY) {
        mpd_sub(profit, price, order->price, &mpd_ctx);
    } else {
        mpd_sub(profit, order->price, price, &mpd_ctx);
    }
    mpd_mul(profit, profit, order->lot, &mpd_ctx);

    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        mpd_mul(profit, profit, sym->contract_size, &mpd_ctx);
        if (sym->profit_type == PROFIT_TYPE_UB) {
            mpd_div(profit, profit, price, &mpd_ctx);        // USDJPY
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

    if (real) {
        // 2.update float
        balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);
    }

    // 3.update order
    mpd_copy(order->profit, profit, &mpd_ctx);
    mpd_copy(order->close_price, price, &mpd_ctx);
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    // 4.update equity,margin,margin_free
    balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
    balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        balance_sub_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_sub_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    }

    // 5.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        balance = balance_sub_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL)
        return -__LINE__;

    int ret = 0;

    // append history
    if (real) {
        double now = current_timestamp();
        ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
        if (ret < 0) {
            log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    if (real) {
        ret = finish_position(order);
        if (ret < 0) {
            log_fatal("tpsl_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
//        push_order_message(ORDER_EVENT_FINISH, order, m);
    }

    order_tpsl(m, order);
    return 0;
}

int market_update(bool real, json_t **result, market_t *m, order_t *order, mpd_t *tp, mpd_t *sl)
{
    int delete_tp = mpd_cmp(order->tp, tp, &mpd_ctx);
    int delete_sl = mpd_cmp(order->sl, sl, &mpd_ctx);

    if (order->side == ORDER_SIDE_SELL) {
        if (delete_tp != 0) {
            skiplist_node *tp_node = skiplist_find(m->tp_sells, order);
            if (tp_node) {
                skiplist_delete(m->tp_sells, tp_node);
            }
        }
        if (delete_sl != 0) {
            skiplist_node *sl_node = skiplist_find(m->sl_sells, order);
            if (sl_node) {
                skiplist_delete(m->sl_sells, sl_node);
            }
        }
    } else {
        if (delete_tp != 0) {
            skiplist_node *tp_node = skiplist_find(m->tp_buys, order);
            if (tp_node) {
                skiplist_delete(m->tp_buys, tp_node);
            }
        }
        if (delete_sl != 0) {
            skiplist_node *sl_node = skiplist_find(m->sl_buys, order);
            if (sl_node) {
                skiplist_delete(m->sl_buys, sl_node);
            }
        }
    }

    mpd_copy(order->tp, tp, &mpd_ctx);
    mpd_copy(order->sl, sl, &mpd_ctx);

    if (mpd_cmp(tp, mpd_zero, &mpd_ctx) > 0) {
        if (order->side == ORDER_SIDE_BUY) {
            if (delete_tp != 0) {
                if (skiplist_insert(m->tp_buys, order) == NULL)
                    return -__LINE__;
            }
        } else {
            if (delete_tp != 0) {
                if (skiplist_insert(m->tp_sells, order) == NULL)
                    return -__LINE__;
            }
        }
    }
    if (mpd_cmp(sl, mpd_zero, &mpd_ctx) > 0) {
        if (order->side == ORDER_SIDE_BUY) {
            if (delete_sl != 0) {
                if (skiplist_insert(m->sl_buys, order) == NULL)
                    return -__LINE__;
            }
        } else {
            if (delete_sl != 0) {
                if (skiplist_insert(m->sl_sells, order) == NULL)
                    return -__LINE__;
            }
        }
    }

    if (real) {
        push_order_message_v2(ORDER_EVENT_UPDATE, order);
        *result = get_order_info_v2(order);
    }
    return 0;
}

static void append_stop_out_log(order_t *order)
{
    json_t *params = json_array();
    json_array_append_new(params, json_integer(order->sid));
    json_array_append_new(params, json_string(order->symbol));
    json_array_append_new(params, json_integer(order->id));
    json_array_append_new(params, json_string(order->comment));
    json_array_append_new(params, json_string(rstripzero(mpd_to_sci(order->close_price, 0))));
    json_array_append_new(params, json_string(rstripzero(mpd_to_sci(order->profit_price, 0))));
    json_array_append_new(params, json_real(order->finish_time));
    append_operlog("stop_out_order", params);
}

int market_stop_out(bool real, market_t *m, uint64_t sid, order_t *order, const char *comment, double finish_time)
{
    mpd_t *profit = mpd_new(&mpd_ctx);
    mpd_copy(profit, order->profit, &mpd_ctx);

    if (real) {
        // 1.update float
        balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);
    }

    // 2.update order
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    // 3.update equity,margin,margin_free
    balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
    balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        balance_sub_float(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
    }

    // 4.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        balance = balance_sub_float(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL)
        return -__LINE__;

    int ret = 0;

    // append history
    if (real) {
        double now = current_timestamp();
        ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
        if (ret < 0) {
            log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    if (real) {
        ret = finish_position(order);
        if (ret < 0) {
            log_fatal("finish_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    // append operlog
    append_stop_out_log(order);

    order_finish_v2(m, order);
    return 0;
}

// hedged margin
static mpd_t *market_get_margin(market_t *m, uint64_t sid)
{
    struct dict_sid_key key = { .sid = sid };
    dict_entry *entry = dict_find(m->margins, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

static void market_set_margin(market_t *m, uint64_t sid, mpd_t *margin)
{
    struct dict_sid_key key = { .sid = sid };
    dict_entry *entry = dict_find(m->margins, &key);
    if (entry) {
        mpd_copy(entry->val, margin, &mpd_ctx);
    } else {
        dict_add(m->margins, &key, margin);
    }
}

int market_open_hedged(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot,
                mpd_t *tp, mpd_t *sl, mpd_t *percentage, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, mpd_t *margin_price, double create_time)
{
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0) {
       return -3;
    }
    if (mpd_cmp(margin_price, mpd_zero, &mpd_ctx) <= 0) {
       return -4;
    }

    // 1.计算保证金, c = contract_size / 100
    // Forex = lots * contract_size / leverage * percentage / 100
    // CFD = lots * contract_size / leverage * percentage / 100 * market_price
    mpd_t *margin = mpd_new(&mpd_ctx);
    mpd_set_u32(margin, leverage, &mpd_ctx);
    mpd_div(margin, sym->c, margin, &mpd_ctx);
    mpd_mul(margin, margin, percentage, &mpd_ctx);
    mpd_mul(margin, margin, lot, &mpd_ctx);

    if (sym->margin_calc == MARGIN_CALC_FOREX) {
        if (sym->margin_type == MARGIN_TYPE_AU) {
            mpd_mul(margin, margin, price, &mpd_ctx);        // EURUSD
        } else if (sym->margin_type == MARGIN_TYPE_AC) {
            mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURGBP
        } else if (sym->margin_type == MARGIN_TYPE_BC) {
            mpd_div(margin, margin, margin_price, &mpd_ctx); // CADJPY
        }
    } else {
         if (strcmp(sym->name, "HSI") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDHKD
         } else if (strcmp(sym->name, "DAX") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURUSD
         } else if (strcmp(sym->name, "UK100") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // GBPUSD
         } else if (strcmp(sym->name, "JP225") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDJPY
         }
         mpd_mul(margin, margin, price, &mpd_ctx);
    }
    mpd_rescale(margin, margin, -2, &mpd_ctx);

    // 2.检查对冲订单
    mpd_t *cur_margin = market_get_margin(m, sid);
    if (cur_margin == NULL) {
        cur_margin = mpd_new(&mpd_ctx);
        skiplist_t *list = market_get_order_list_v2(m, sid);
        if (list != NULL) {
            // 说明系统重启过，需要恢复cur_margin
            mpd_t *buy_margin = mpd_new(&mpd_ctx);
            mpd_t *sell_margin = mpd_new(&mpd_ctx);
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(buy_margin, mpd_zero, &mpd_ctx);
            mpd_copy(sell_margin, mpd_zero, &mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);

            skiplist_iter *it = skiplist_get_iterator(list);
            skiplist_node *node;
            while ((node = skiplist_next(it)) != NULL) {
                order_t *order = node->value;
                if (order->side == ORDER_SIDE_BUY) {
                    mpd_add(temp, temp, order->lot, &mpd_ctx);
                    mpd_add(buy_margin, buy_margin, order->margin, &mpd_ctx);
                } else {
                    mpd_sub(temp, temp, order->lot, &mpd_ctx);
                    mpd_sub(sell_margin, sell_margin, order->margin, &mpd_ctx);
                }
            }
            skiplist_release_iterator(it);

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                mpd_copy(cur_margin, sell_margin, &mpd_ctx);
            else
                mpd_copy(cur_margin, buy_margin, &mpd_ctx);

            mpd_del(buy_margin);
            mpd_del(sell_margin);
            mpd_del(temp);
        } else {
            mpd_copy(cur_margin, mpd_zero, &mpd_ctx);
        }
        market_set_margin(m, sid, cur_margin);
    }

    int cmp = mpd_cmp(cur_margin, mpd_zero, &mpd_ctx);
    mpd_t *update_margin = mpd_new(&mpd_ctx);
    mpd_copy(update_margin, margin, &mpd_ctx);

    // 0-不变，1-buy累加，2-sell累减，3-变sell单边，4-变buy单边
    int action = 0;
//log_info("## cur_margin1 = %s", mpd_to_sci(cur_margin, 0));

    if (cmp == 0) {
        // 2.1 当前无持仓单
        // buy:  cm + m, M + m
        // sell: cm - m, M + m
        if (side == ORDER_SIDE_BUY)
            action = 1;
        else
            action = 2;
    }
    else if (cmp > 0) {
        // 2.2 buy单边
        // buy:  cm + m, M + m
        // sell blots >= slots: cm, M
        // sell blots < slots: cm = -Ms, M - cm + Ms
        if (side == ORDER_SIDE_BUY) {
            action = 1;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_sub(temp, temp, lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *order = node->value;
                    if (order->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, order->lot, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, order->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, order->margin, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

//log_info("## buy temp = %s", mpd_to_sci(temp, 0));
//log_info("## buy update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 3;
            else
                action = 0;

            mpd_del(temp);
        }
    } else if (cmp < 0) {
        // 2.3 sell单边
        // sell: cm - m,  M + m
        // buy blots < slots: cm, M
        // buy blots >= slots: cm = Mb, M - cm + Mb
        if (side == ORDER_SIDE_SELL) {
            action = 2;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_add(temp, mpd_zero, lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *order = node->value;
                    if (order->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, order->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, order->margin, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, order->lot, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

//log_info("## sell temp = %s", mpd_to_sci(temp, 0));
//log_info("## sell update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 0;
            else
                action = 4; // 默认收取buy单边的保证金，方便系统重启恢复

            mpd_del(temp);
        }
    }

//log_info("## action = %d", action);
//log_info("## cur_margin2 = %s", mpd_to_sci(cur_margin, 0));
//log_info("## update_margin = %s", mpd_to_sci(update_margin, 0));

    // 3.判断 可用金 + 浮动盈亏 是否足够
    mpd_t *free = balance_get_v2(sid, BALANCE_TYPE_FREE);
    mpd_t *floa = balance_get_v2(sid, BALANCE_TYPE_FLOAT);
    mpd_t *pnl = mpd_new(&mpd_ctx);
    if (free) {
        mpd_copy(pnl, free, &mpd_ctx);
        if (floa)
            mpd_add(pnl, pnl, floa, &mpd_ctx);
    } else {
        if (floa)
            mpd_copy(pnl, floa, &mpd_ctx);
    }
//    if (free) {
    if (pnl && mpd_cmp(pnl, mpd_zero, &mpd_ctx) > 0) {
        mpd_t *total = mpd_new(&mpd_ctx);
        if (action == 0) {
//            if (mpd_cmp(free, fee, &mpd_ctx) > 0) {
            if (mpd_cmp(pnl, fee, &mpd_ctx) > 0) {
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, fee);
                mpd_del(total);
            } else {
                log_info("## free = %s, pnl = %s, fee = %s",  mpd_to_sci(free, 0), mpd_to_sci(pnl, 0), mpd_to_sci(fee, 0));
                mpd_del(margin);
                mpd_del(total);
                mpd_del(update_margin);
                return -2;
            }
        } else if (action < 3) {
            mpd_copy(total, margin, &mpd_ctx);
            mpd_add(total, margin, fee, &mpd_ctx);
//            if (mpd_cmp(free, total, &mpd_ctx) > 0) {
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                if (action == 1) {
                    mpd_add(cur_margin, cur_margin, margin, &mpd_ctx);
                } else {
                    mpd_sub(cur_margin, cur_margin, margin, &mpd_ctx);
                }
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, pnl = %s, margin = %s, total = %s", mpd_to_sci(free, 0),  mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                mpd_del(margin);
                mpd_del(total);
                mpd_del(update_margin);
                return -2;
            }
        } else if (action == 3) {
            mpd_sub(total, update_margin, cur_margin, &mpd_ctx);
            mpd_add(total, total, fee, &mpd_ctx);
//            if (mpd_cmp(free, total, &mpd_ctx) > 0) {
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);
                balance_sub_v2(sid, BALANCE_TYPE_MARGIN, cur_margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                mpd_sub(cur_margin, mpd_zero, update_margin, &mpd_ctx);
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, pnl = %s, margin = %s, total = %s", mpd_to_sci(free, 0),  mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                mpd_del(margin);
                mpd_del(total);
                mpd_del(update_margin);
                return -2;
            }
        } else {
            mpd_add(total, update_margin, cur_margin, &mpd_ctx);
            mpd_add(total, total, fee, &mpd_ctx);
//            if (mpd_cmp(free, total, &mpd_ctx) > 0) {
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);
                balance_add_float(sid, BALANCE_TYPE_MARGIN, cur_margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                mpd_copy(cur_margin, update_margin, &mpd_ctx);
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, margin = %s, total = %s", mpd_to_sci(free, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                mpd_del(margin);
                mpd_del(total);
                mpd_del(update_margin);
                return -2;
            }
        }
    } else {
        mpd_del(margin);
        mpd_del(update_margin);
        return -2;
    }

    mpd_del(pnl);
    mpd_del(update_margin);
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_MARKET;
    order->side         = side;
    order->create_time  = create_time;
    order->update_time  = 0;
    order->finish_time  = 0;
    order->expire_time  = 0;
    order->sid          = sid;
    order->external     = external;
    order->symbol       = strdup(m->name);
    order->comment      = strdup(comment);

    order->price        = mpd_new(&mpd_ctx);
    order->lot          = mpd_new(&mpd_ctx);
    order->close_price  = mpd_new(&mpd_ctx);
    order->margin       = mpd_new(&mpd_ctx);
    order->fee          = mpd_new(&mpd_ctx);
    order->swap         = mpd_new(&mpd_ctx);
    order->swaps        = mpd_new(&mpd_ctx);
    order->profit       = mpd_new(&mpd_ctx);
    order->tp           = mpd_new(&mpd_ctx);
    order->sl           = mpd_new(&mpd_ctx);
    order->margin_price = mpd_new(&mpd_ctx);
    order->profit_price = mpd_new(&mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->lot, lot, &mpd_ctx);
    mpd_copy(order->fee, fee, &mpd_ctx);
    mpd_copy(order->swap, swap, &mpd_ctx);
    mpd_copy(order->swaps, mpd_zero, &mpd_ctx);
    mpd_copy(order->tp, tp, &mpd_ctx);
    mpd_copy(order->sl, sl, &mpd_ctx);
    mpd_copy(order->close_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->margin, margin, &mpd_ctx);
    mpd_copy(order->profit, mpd_zero, &mpd_ctx);
    mpd_copy(order->margin_price, margin_price, &mpd_ctx);
    mpd_copy(order->profit_price, mpd_one, &mpd_ctx);

    mpd_del(margin);

    int ret = order_put_v2(m, order);
    if (ret < 0) {
        log_fatal("order_put fail: %d, order: %"PRIu64"", ret, order->id);
        return ret;
    }
    if (real) {
        ret = append_position(order);
        if (ret < 0) {
            log_fatal("append_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message_v2(ORDER_EVENT_OPEN, order);
        *result = get_order_info_v2(order);
    }

    return 0;
}

int market_close_hedged(bool real, json_t **result, market_t *m, symbol_t *sym, uint64_t sid, order_t *order, mpd_t *price, const char *comment, mpd_t *profit_price, double finish_time)
{
    if (mpd_cmp(price, mpd_zero, &mpd_ctx) <= 0) {
       return -3;
    }
    if (mpd_cmp(profit_price, mpd_zero, &mpd_ctx) <= 0) {
       return -5;
    }

    // 1.calaulate profit
    // Forex / CFD = (close_price - open_price) * contract_size * lots
    // Futures = (close_price - open_price) * tick_price / tick_size * lots
    mpd_t *profit = mpd_new(&mpd_ctx);
    if (order->side == ORDER_SIDE_BUY) {
        mpd_sub(profit, price, order->price, &mpd_ctx);
    } else {
        mpd_sub(profit, order->price, price, &mpd_ctx);
    }
    mpd_mul(profit, profit, order->lot, &mpd_ctx);

    if (sym->profit_calc == PROFIT_CALC_FOREX) {
        mpd_mul(profit, profit, sym->contract_size, &mpd_ctx);
        if (sym->profit_type == PROFIT_TYPE_UB) {
            mpd_div(profit, profit, price, &mpd_ctx);        // USDJPY
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

    // 2.update float
    if (real && mpd_cmp(order->profit, mpd_zero, &mpd_ctx) != 0) {
        balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);
    }

    // 3.update order
    mpd_copy(order->profit, profit, &mpd_ctx);
    mpd_copy(order->close_price, price, &mpd_ctx);
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    // 4.udpate margin
    mpd_t *cur_margin = market_get_margin(m, sid);
    if (cur_margin == NULL) {
        cur_margin = mpd_new(&mpd_ctx);
        skiplist_t *list = market_get_order_list_v2(m, sid);
        if (list != NULL) {
            // 说明系统重启过，需要恢复cur_margin
            mpd_t *buy_margin = mpd_new(&mpd_ctx);
            mpd_t *sell_margin = mpd_new(&mpd_ctx);
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(buy_margin, mpd_zero, &mpd_ctx);
            mpd_copy(sell_margin, mpd_zero, &mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);

            skiplist_iter *it = skiplist_get_iterator(list);
            skiplist_node *node;
            while ((node = skiplist_next(it)) != NULL) {
                order_t *o = node->value;
                if (o->side == ORDER_SIDE_BUY) {
                    mpd_add(temp, temp, o->lot, &mpd_ctx);
                    mpd_add(buy_margin, buy_margin, o->margin, &mpd_ctx);
                } else {
                    mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    mpd_sub(sell_margin, sell_margin, o->margin, &mpd_ctx);
                }
            }
            skiplist_release_iterator(it);

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                mpd_copy(cur_margin, sell_margin, &mpd_ctx);
            else
                mpd_copy(cur_margin, buy_margin, &mpd_ctx);

            mpd_del(buy_margin);
            mpd_del(sell_margin);
            mpd_del(temp);
        } else {
            mpd_copy(cur_margin, mpd_zero, &mpd_ctx);
        }
        market_set_margin(m, sid, cur_margin);
    }

    int cmp = mpd_cmp(cur_margin, mpd_zero, &mpd_ctx);
    mpd_t *update_margin = mpd_new(&mpd_ctx);
    mpd_copy(update_margin, mpd_zero, &mpd_ctx);

    // 0-不变，1-buy累减，2-sell累加，3-变sell单边，4-变buy单边
    int action = 0;
log_info("## cur_margin1 = %s", mpd_to_sci(cur_margin, 0));

    if (cmp > 0) {
        // 4.1 buy单边
        // sell:  cm, M
        // buy blots >= slots: cm - m, M - m
        // buy blots < slots: cm = -Ms, M - cm + Ms
        if (order->side == ORDER_SIDE_SELL) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_sub(temp, temp, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## buy temp = %s", mpd_to_sci(temp, 0));
log_info("## buy update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 3;
            else
                action = 1;

            mpd_del(temp);
        }
    } else if (cmp < 0) {
        // 4.2 sell单边
        // buy: cm, M
        // sell blots < slots: cm + m, M - m
        // sell blots >= slots: cm = Mb, M - cm + Mb
        if (order->side == ORDER_SIDE_BUY) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_add(temp, mpd_zero, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## sell temp = %s", mpd_to_sci(temp, 0));
log_info("## sell update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 2;
            else
                action = 4;

            mpd_del(temp);
        }
    }

log_info("## action = %d", action);
log_info("## cur_margin2 = %s", mpd_to_sci(cur_margin, 0));
log_info("## update_margin = %s", mpd_to_sci(update_margin, 0));

    // 4.3 更新保证金
    if (action == 1) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_sub(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 2) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_add(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 3) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_add_v2(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_sub(cur_margin, mpd_zero, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);

    } else if (action == 4) {
        balance_add_float(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_sub_float(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_copy(cur_margin, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    }

log_info("## cur_margin3 = %s", mpd_to_sci(cur_margin, 0));
    mpd_del(update_margin);

    // 5.update profit
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        if (real) {
            balance_sub_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
            balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
        } else {
            // 穿仓是负数
            balance_sub_float(sid, BALANCE_TYPE_EQUITY, abs_profit);
            balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
        }
    }

    // 6.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        balance = balance_sub_float(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL) {
        mpd_del(profit);
        mpd_del(abs_profit);
        return -__LINE__;
    }

    int ret = 0;
    // append history
    if (real) {
        double now = current_timestamp();
        ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
        if (ret < 0) {
            log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    if (real) {
        ret = finish_position(order);
        if (ret < 0) {
            log_fatal("finish_position fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message_v2(ORDER_EVENT_CLOSE, order);
        *result = get_order_info_v2(order);
    }

    order_finish_v2(m, order);
    return 0;
}

static void append_tpsl_log(order_t *order)
{
    json_t *params = json_array();
    json_array_append_new(params, json_integer(order->sid));
    json_array_append_new(params, json_string(order->symbol));
    json_array_append_new(params, json_integer(order->id));
    json_array_append_new(params, json_string(order->comment));
    json_array_append_new(params, json_string(rstripzero(mpd_to_sci(order->close_price, 0))));
    json_array_append_new(params, json_string(rstripzero(mpd_to_sci(order->profit_price, 0))));
    json_array_append_new(params, json_real(order->finish_time));
    append_operlog("tpsl_order", params);
}

int market_tpsl_hedged(market_t *m, uint64_t order_id)
{
    order_t *order = market_get_order(m, order_id);
    if (order == NULL)
        return -__LINE__;
    uint64_t sid = order->sid;

    // 1.update float
    balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);

    // 2.udpate margin
    mpd_t *cur_margin = market_get_margin(m, sid);
    if (cur_margin == NULL) {
        cur_margin = mpd_new(&mpd_ctx);
        skiplist_t *list = market_get_order_list_v2(m, sid);
        if (list != NULL) {
            // 说明系统重启过，需要恢复cur_margin
            mpd_t *buy_margin = mpd_new(&mpd_ctx);
            mpd_t *sell_margin = mpd_new(&mpd_ctx);
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(buy_margin, mpd_zero, &mpd_ctx);
            mpd_copy(sell_margin, mpd_zero, &mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);

            skiplist_iter *it = skiplist_get_iterator(list);
            skiplist_node *node;
            while ((node = skiplist_next(it)) != NULL) {
                order_t *o = node->value;
                if (o->side == ORDER_SIDE_BUY) {
                    mpd_add(temp, temp, o->lot, &mpd_ctx);
                    mpd_add(buy_margin, buy_margin, o->margin, &mpd_ctx);
                } else {
                    mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    mpd_sub(sell_margin, sell_margin, o->margin, &mpd_ctx);
                }
            }
            skiplist_release_iterator(it);

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                mpd_copy(cur_margin, sell_margin, &mpd_ctx);
            else
                mpd_copy(cur_margin, buy_margin, &mpd_ctx);

            mpd_del(buy_margin);
            mpd_del(sell_margin);
            mpd_del(temp);
        } else {
            mpd_copy(cur_margin, mpd_zero, &mpd_ctx);
        }
        market_set_margin(m, sid, cur_margin);
    }

    int cmp = mpd_cmp(cur_margin, mpd_zero, &mpd_ctx);
    mpd_t *update_margin = mpd_new(&mpd_ctx);
    mpd_copy(update_margin, mpd_zero, &mpd_ctx);

    // 0-不变，1-buy累减，2-sell累加，3-变sell单边，4-变buy单边
    int action = 0;
log_info("## cur_margin1 = %s", mpd_to_sci(cur_margin, 0));

    if (cmp > 0) {
        // 2.1 buy单边
        // sell:  cm, M
        // buy blots >= slots: cm - m, M - m
        // buy blots < slots: cm = -Ms, M - cm + Ms
        if (order->side == ORDER_SIDE_SELL) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_sub(temp, temp, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## buy temp = %s", mpd_to_sci(temp, 0));
log_info("## buy update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 3;
            else
                action = 1;

            mpd_del(temp);
        }
    } else if (cmp < 0) {
        // 2.2 sell单边
        // buy: cm, M
        // sell blots < slots: cm + m, M - m
        // sell blots >= slots: cm = Mb, M - cm + Mb
        if (order->side == ORDER_SIDE_BUY) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_add(temp, mpd_zero, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## sell temp = %s", mpd_to_sci(temp, 0));
log_info("## sell update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 2;
            else
                action = 4;

            mpd_del(temp);
        }
    }

log_info("## action = %d", action);
log_info("## cur_margin2 = %s", mpd_to_sci(cur_margin, 0));
log_info("## update_margin = %s", mpd_to_sci(update_margin, 0));

    // 2.3 更新保证金
    if (action == 1) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_sub(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 2) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_add(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 3) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_add_v2(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_sub(cur_margin, mpd_zero, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);

    } else if (action == 4) {
        balance_add_float(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_sub_float(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_copy(cur_margin, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    }

log_info("## cur_margin3 = %s", mpd_to_sci(cur_margin, 0));
    mpd_del(update_margin);

    // 3.update profit
    mpd_t *profit = mpd_new(&mpd_ctx);
    mpd_copy(profit, order->profit, &mpd_ctx);
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        balance_sub_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
    }

    // 4.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        balance = balance_sub_float(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL) {
        mpd_del(profit);
        mpd_del(abs_profit);
        return -__LINE__;
    }

    // append history
    double now = current_timestamp();
    int ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
    if (ret < 0) {
        log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    ret = finish_position(order);
    if (ret < 0) {
        log_fatal("finish_position fail: %d, order: %"PRIu64"", ret, order->id);
    }
    push_order_message_v2(ORDER_EVENT_TPSL, order);

    append_tpsl_log(order);
    order_finish_v2(m, order);
    return 0;
}

int market_stop_out_hedged(market_t *m, uint64_t sid, order_t *order, const char *comment, double finish_time)
{
    // 1.update float
    balance_sub_float(sid, BALANCE_TYPE_FLOAT, order->profit);

    // 2.udpate margin
    mpd_t *cur_margin = market_get_margin(m, sid);
    if (cur_margin == NULL) {
        cur_margin = mpd_new(&mpd_ctx);
        skiplist_t *list = market_get_order_list_v2(m, sid);
        if (list != NULL) {
            // 说明系统重启过，需要恢复cur_margin
            mpd_t *buy_margin = mpd_new(&mpd_ctx);
            mpd_t *sell_margin = mpd_new(&mpd_ctx);
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(buy_margin, mpd_zero, &mpd_ctx);
            mpd_copy(sell_margin, mpd_zero, &mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);

            skiplist_iter *it = skiplist_get_iterator(list);
            skiplist_node *node;
            while ((node = skiplist_next(it)) != NULL) {
                order_t *o = node->value;
                if (o->side == ORDER_SIDE_BUY) {
                    mpd_add(temp, temp, o->lot, &mpd_ctx);
                    mpd_add(buy_margin, buy_margin, o->margin, &mpd_ctx);
                } else {
                    mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    mpd_sub(sell_margin, sell_margin, o->margin, &mpd_ctx);
                }
            }
            skiplist_release_iterator(it);

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                mpd_copy(cur_margin, sell_margin, &mpd_ctx);
            else
                mpd_copy(cur_margin, buy_margin, &mpd_ctx);

            mpd_del(buy_margin);
            mpd_del(sell_margin);
            mpd_del(temp);
        } else {
            mpd_copy(cur_margin, mpd_zero, &mpd_ctx);
        }
        market_set_margin(m, sid, cur_margin);
    }

    int cmp = mpd_cmp(cur_margin, mpd_zero, &mpd_ctx);
    mpd_t *update_margin = mpd_new(&mpd_ctx);
    mpd_copy(update_margin, mpd_zero, &mpd_ctx);

    // 0-不变，1-buy累减，2-sell累加，3-变sell单边，4-变buy单边
    int action = 0;
log_info("## cur_margin1 = %s", mpd_to_sci(cur_margin, 0));

    if (cmp > 0) {
        // 2.1 buy单边
        // sell:  cm, M
        // buy blots >= slots: cm - m, M - m
        // buy blots < slots: cm = -Ms, M - cm + Ms
        if (order->side == ORDER_SIDE_SELL) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_sub(temp, temp, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## buy temp = %s", mpd_to_sci(temp, 0));
log_info("## buy update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 3;
            else
                action = 1;

            mpd_del(temp);
        }
    } else if (cmp < 0) {
        // 2.2 sell单边
        // buy: cm, M
        // sell blots < slots: cm + m, M - m
        // sell blots >= slots: cm = Mb, M - cm + Mb
        if (order->side == ORDER_SIDE_BUY) {
            action = 0;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_add(temp, mpd_zero, order->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *o = node->value;
                    if (o->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, o->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, o->margin, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, o->lot, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## sell temp = %s", mpd_to_sci(temp, 0));
log_info("## sell update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 2;
            else
                action = 4;

            mpd_del(temp);
        }
    }

log_info("## action = %d", action);
log_info("## cur_margin2 = %s", mpd_to_sci(cur_margin, 0));
log_info("## update_margin = %s", mpd_to_sci(update_margin, 0));

    // 2.3 更新保证金
    if (action == 1) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_sub(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 2) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, order->margin);
        balance_add_v2(sid, BALANCE_TYPE_FREE, order->margin);
        mpd_add(cur_margin, cur_margin, order->margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    } else if (action == 3) {
        balance_sub_v2(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_add_v2(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_sub(cur_margin, mpd_zero, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);

    } else if (action == 4) {
        balance_add_float(sid, BALANCE_TYPE_MARGIN, cur_margin);
        balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);

        balance_sub_float(sid, BALANCE_TYPE_FREE, cur_margin);
        balance_sub_float(sid, BALANCE_TYPE_FREE, update_margin);

        mpd_copy(cur_margin, update_margin, &mpd_ctx);
        market_set_margin(m, sid, cur_margin);
    }

log_info("## cur_margin3 = %s", mpd_to_sci(cur_margin, 0));
    mpd_del(update_margin);

    // 3.update profit
    mpd_t *profit = mpd_new(&mpd_ctx);
    mpd_copy(profit, order->profit, &mpd_ctx);
    mpd_t *abs_profit = mpd_new(&mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance_add_v2(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_add_v2(sid, BALANCE_TYPE_FREE, abs_profit);
    } else {
        // 穿仓是负数
        balance_sub_float(sid, BALANCE_TYPE_EQUITY, abs_profit);
        balance_sub_float(sid, BALANCE_TYPE_FREE, abs_profit);
    }

    // 4.update balance
    mpd_sub(profit, profit, order->fee, &mpd_ctx);
    mpd_sub(profit, profit, order->swaps, &mpd_ctx);
    mpd_abs(abs_profit, profit, &mpd_ctx);

    mpd_t *balance;
    if (mpd_cmp(profit, mpd_zero, &mpd_ctx) > 0) {
        balance = balance_add_v2(sid, BALANCE_TYPE_BALANCE, abs_profit);
    } else {
        // 穿仓是负数
        balance = balance_sub_float(sid, BALANCE_TYPE_BALANCE, abs_profit);
    }

    if (balance == NULL) {
        mpd_del(profit);
        mpd_del(abs_profit);
        return -__LINE__;
    }

    // 5.update order
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    // append history
    double now = current_timestamp();
    int ret = append_user_balance_history_v2(now, sid, order->id, BUSINESS_TYPE_TRADE, profit, balance, "");
    if (ret < 0) {
        log_fatal("append_user_balance_history_v2 fail: %d, order: %"PRIu64"", ret, order->id);
    }

    mpd_del(profit);
    mpd_del(abs_profit);

    ret = finish_position(order);
    if (ret < 0)
        log_fatal("finish_position fail: %d, order: %"PRIu64"", ret, order->id);
    push_order_message_v2(ORDER_EVENT_STOP, order);

    // append operlog
    append_stop_out_log(order);
    order_finish_v2(m, order);

    return 0;
}

static int limit_put(market_t *m, order_t *order)
{
    struct dict_order_key order_key = { .order_id = order->id };
    if (dict_add(m->limit_orders, &order_key, order) == NULL)
        return -__LINE__;

    struct dict_sid_key sid_key = { .sid = order->sid };
    dict_entry *entry = dict_find(m->limit_users, &sid_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
    } else {
        skiplist_type type;
        memset(&type, 0, sizeof(type));
        type.compare = order_id_compare;
        skiplist_t *order_list = skiplist_create(&type);
        if (order_list == NULL)
            return -__LINE__;
        if (skiplist_insert(order_list, order) == NULL)
            return -__LINE__;
        if (dict_add(m->limit_users, &sid_key, order_list) == NULL)
            return -__LINE__;
    }

    if (order->side == ORDER_SIDE_BUY) {
        if (skiplist_insert(m->limit_buys, order) == NULL)
            return -__LINE__;
    } else {
        if (skiplist_insert(m->limit_sells, order) == NULL)
            return -__LINE__;
    }

    if (order->expire_time > 0) {
        if (skiplist_insert(expire_orders, order) == NULL)
            return -__LINE__;
    }

    return 0;
}

order_t *market_get_limit(market_t *m, uint64_t order_id)
{
    struct dict_order_key key = { .order_id = order_id };
    dict_entry *entry = dict_find(m->limit_orders, &key);
    if (entry) {
        return entry->val;
    }
    return NULL;
}

static int order_cancel(market_t *m, order_t *order, bool free)
{
    if (order->side == ORDER_SIDE_SELL) {
        skiplist_node *node = skiplist_find(m->limit_sells, order);
        if (node) {
            skiplist_delete(m->limit_sells, node);
        }
    } else {
        skiplist_node *node = skiplist_find(m->limit_buys, order);
        if (node) {
            skiplist_delete(m->limit_buys, node);
        }
    }

    struct dict_order_key order_key = { .order_id = order->id };
    dict_delete(m->limit_orders, &order_key);

    struct dict_sid_key sid_key = { .sid = order->sid };
    dict_entry *entry = dict_find(m->limit_users, &sid_key);
    if (entry) {
        skiplist_t *order_list = entry->val;
        skiplist_node *node = skiplist_find(order_list, order);
        if (node) {
            skiplist_delete(order_list, node);
        }
    }

    if (order->expire_time > 0) {
        skiplist_node *node = skiplist_find(expire_orders, order);
        if (node) {
            skiplist_delete(expire_orders, node);
        }
    }

    if (free) {
        order_free_v2(order);
    }
    return 0;
}

int market_cancel(bool real, json_t **result, market_t *m, order_t *order, const char *comment, double finish_time)
{
    mpd_copy(order->margin, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->swap, mpd_zero, &mpd_ctx);
    order->comment = strdup(comment);
    order->finish_time = finish_time;

    if (real) {
        int ret = finish_limit(order, true);
        if (ret < 0) {
            log_fatal("finish_limit fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message(ORDER_EVENT_CANCEL, order, m);
        *result = get_order_info_v2(order);
    }
    order_cancel(m, order, true);
    return 0;
}

int market_put_limit(bool real, json_t **result, market_t *m, uint64_t sid, uint32_t leverage, uint32_t side, mpd_t *price, mpd_t *lot, mpd_t *tp,
		mpd_t *sl, mpd_t *percentage, mpd_t *fee, mpd_t *swap, uint64_t external, const char *comment, double create_time, uint64_t expire_time)
{
    order_t *order = malloc(sizeof(order_t));
    if (order == NULL) {
        return -__LINE__;
    }

    order->id           = ++order_id_start;
    order->type         = MARKET_ORDER_TYPE_LIMIT;
    order->side         = side;
    order->create_time  = create_time;
    order->update_time  = 0;
    order->finish_time  = 0;
    order->expire_time  = expire_time;
    order->sid          = sid;
    order->external     = external;
    order->symbol       = strdup(m->name);
    order->comment      = strdup(comment);

    order->price        = mpd_new(&mpd_ctx);
    order->lot          = mpd_new(&mpd_ctx);
    order->close_price  = mpd_new(&mpd_ctx);
    order->margin       = mpd_new(&mpd_ctx);
    order->fee          = mpd_new(&mpd_ctx);
    order->swap         = mpd_new(&mpd_ctx);
    order->swaps        = mpd_new(&mpd_ctx);
    order->profit       = mpd_new(&mpd_ctx);
    order->tp           = mpd_new(&mpd_ctx);
    order->sl           = mpd_new(&mpd_ctx);
    order->margin_price = mpd_new(&mpd_ctx);
    order->profit_price = mpd_new(&mpd_ctx);

    // 保存 percentage / leverage 到保证金字段
    mpd_set_u32(order->margin, leverage, &mpd_ctx);
    mpd_div(order->margin, percentage, order->margin, &mpd_ctx);

    mpd_copy(order->price, price, &mpd_ctx);
    mpd_copy(order->lot, lot, &mpd_ctx);
    mpd_copy(order->fee, fee, &mpd_ctx);
    mpd_copy(order->swap, swap, &mpd_ctx);
    mpd_copy(order->swaps, mpd_zero, &mpd_ctx);
    mpd_copy(order->tp, tp, &mpd_ctx);
    mpd_copy(order->sl, sl, &mpd_ctx);
    mpd_copy(order->close_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->profit, mpd_zero, &mpd_ctx);
    mpd_copy(order->margin_price, mpd_zero, &mpd_ctx);
    mpd_copy(order->profit_price, mpd_one, &mpd_ctx);

    int ret = limit_put(m, order);
    if (ret < 0) {
        log_fatal("limit_put fail: %d, order: %"PRIu64"", ret, order->id);
        return ret;
    }
    if (real) {
        ret = append_limit(order);
        if (ret < 0) {
            log_fatal("append_limit fail: %d, order: %"PRIu64"", ret, order->id);
        }
        push_order_message_v2(ORDER_EVENT_LIMIT, order);
        *result = get_order_info_v2(order);
    }

    return 0;
}

int market_put_pending(market_t *m, order_t *order)
{
    return limit_put(m, order);
}

int limit_open(bool real, market_t *m, symbol_t *sym, order_t *o, uint64_t sid, mpd_t *price, mpd_t *fee, mpd_t *margin_price, double update_time)
{
    // 1.计算保证金, c = contract_size / 100, o->margin = percentage / leverage
    // Forex = lots * contract_size / leverage * percentage / 100
    // CFD = lots * contract_size / leverage * percentage / 100 * market_price
    mpd_t *margin = mpd_new(&mpd_ctx);
    mpd_copy(margin, o->margin, &mpd_ctx);
    mpd_mul(margin, sym->c, margin, &mpd_ctx);
    mpd_mul(margin, margin, o->lot, &mpd_ctx);

    if (sym->margin_calc == MARGIN_CALC_FOREX) {
        if (sym->margin_type == MARGIN_TYPE_AU) {
            mpd_mul(margin, margin, price, &mpd_ctx);        // EURUSD
        } else if (sym->margin_type == MARGIN_TYPE_AC) {
            mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURGBP
        } else if (sym->margin_type == MARGIN_TYPE_BC) {
            mpd_div(margin, margin, margin_price, &mpd_ctx); // CADJPY
        }
    } else {
         if (strcmp(sym->name, "HSI") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDHKD
         } else if (strcmp(sym->name, "DAX") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // EURUSD
         } else if (strcmp(sym->name, "UK100") == 0) {
             mpd_mul(margin, margin, margin_price, &mpd_ctx); // GBPUSD
         } else if (strcmp(sym->name, "JP225") == 0) {
             mpd_div(margin, margin, margin_price, &mpd_ctx); // USDJPY
         }
         mpd_mul(margin, margin, price, &mpd_ctx);
    }
    mpd_rescale(margin, margin, -2, &mpd_ctx);

    // 2.检查对冲订单
    mpd_t *cur_margin = market_get_margin(m, sid);
    if (cur_margin == NULL) {
        cur_margin = mpd_new(&mpd_ctx);
        skiplist_t *list = market_get_order_list_v2(m, sid);
        if (list != NULL) {
            // 说明系统重启过，需要恢复cur_margin
            mpd_t *buy_margin = mpd_new(&mpd_ctx);
            mpd_t *sell_margin = mpd_new(&mpd_ctx);
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(buy_margin, mpd_zero, &mpd_ctx);
            mpd_copy(sell_margin, mpd_zero, &mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);

            skiplist_iter *it = skiplist_get_iterator(list);
            skiplist_node *node;
            while ((node = skiplist_next(it)) != NULL) {
                order_t *order = node->value;
                if (order->side == ORDER_SIDE_BUY) {
                    mpd_add(temp, temp, order->lot, &mpd_ctx);
                    mpd_add(buy_margin, buy_margin, order->margin, &mpd_ctx);
                } else {
                    mpd_sub(temp, temp, order->lot, &mpd_ctx);
                    mpd_sub(sell_margin, sell_margin, order->margin, &mpd_ctx);
                }
            }
            skiplist_release_iterator(it);

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                mpd_copy(cur_margin, sell_margin, &mpd_ctx);
            else
                mpd_copy(cur_margin, buy_margin, &mpd_ctx);

            mpd_del(buy_margin);
            mpd_del(sell_margin);
            mpd_del(temp);
        } else {
            mpd_copy(cur_margin, mpd_zero, &mpd_ctx);
        }
        market_set_margin(m, sid, cur_margin);
    }

    int cmp = mpd_cmp(cur_margin, mpd_zero, &mpd_ctx);
    mpd_t *update_margin = mpd_new(&mpd_ctx);
    mpd_copy(update_margin, margin, &mpd_ctx);

    // 0-不变，1-buy累加，2-sell累减，3-变sell单边，4-变buy单边
    int action = 0;
    int side = o->side;
log_info("## cur_margin1 = %s", mpd_to_sci(cur_margin, 0));

    if (cmp == 0) {
        // 2.1 当前无持仓单
        // buy:  cm + m, M + m
        // sell: cm - m, M + m
        if (side == ORDER_SIDE_BUY)
            action = 1;
        else
            action = 2;
    }
    else if (cmp > 0) {
        // 2.2 buy单边
        // buy:  cm + m, M + m
        // sell blots >= slots: cm, M
        // sell blots < slots: cm = -Ms, M - cm + Ms
        if (side == ORDER_SIDE_BUY) {
            action = 1;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_sub(temp, temp, o->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *order = node->value;
                    if (order->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, order->lot, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, order->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, order->margin, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## buy temp = %s", mpd_to_sci(temp, 0));
log_info("## buy update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 3;
            else
                action = 0;

            mpd_del(temp);
        }
    } else if (cmp < 0) {
        // 2.3 sell单边
        // sell: cm - m,  M + m
        // buy blots < slots: cm, M
        // buy blots >= slots: cm = Mb, M - cm + Mb
        if (side == ORDER_SIDE_SELL) {
            action = 2;
        } else {
            mpd_t *temp = mpd_new(&mpd_ctx);
            mpd_copy(temp, mpd_zero, &mpd_ctx);
            mpd_add(temp, mpd_zero, o->lot, &mpd_ctx);

            skiplist_t *list = market_get_order_list_v2(m, sid);
            if (list != NULL) {
                skiplist_iter *it = skiplist_get_iterator(list);
                skiplist_node *node;
                while ((node = skiplist_next(it)) != NULL) {
                    order_t *order = node->value;
                    if (order->side == ORDER_SIDE_BUY) {
                        mpd_add(temp, temp, order->lot, &mpd_ctx);
                        mpd_add(update_margin, update_margin, order->margin, &mpd_ctx);
                    } else {
                        mpd_sub(temp, temp, order->lot, &mpd_ctx);
                    }
                }
                skiplist_release_iterator(it);
            }

log_info("## sell temp = %s", mpd_to_sci(temp, 0));
log_info("## sell update_margin = %s", mpd_to_sci(update_margin, 0));

            if (mpd_cmp(temp, mpd_zero, &mpd_ctx) < 0)
                action = 0;
            else
                action = 4; // 默认收取buy单边的保证金，方便系统重启恢复

            mpd_del(temp);
        }
    }

log_info("## action = %d", action);
log_info("## cur_margin2 = %s", mpd_to_sci(cur_margin, 0));
log_info("## update_margin = %s", mpd_to_sci(update_margin, 0));

    // 3.判断 可用金 + 浮动盈亏 是否足够，不够撤单
    int cancel = false;
    mpd_t *free = balance_get_v2(sid, BALANCE_TYPE_FREE);
    mpd_t *floa = balance_get_v2(sid, BALANCE_TYPE_FLOAT);
    mpd_t *pnl = mpd_new(&mpd_ctx);
    if (free) {
        mpd_copy(pnl, free, &mpd_ctx);
        if (floa)
	    mpd_add(pnl, pnl, floa, &mpd_ctx);
    } else {
        if (floa)
            mpd_copy(pnl, floa, &mpd_ctx);
    }
    if (pnl && mpd_cmp(pnl, mpd_zero, &mpd_ctx) > 0) {
        mpd_t *total = mpd_new(&mpd_ctx);
        if (action == 0) {
            if (mpd_cmp(pnl, fee, &mpd_ctx) > 0) {
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, fee);
                mpd_del(total);
            } else {
                log_info("## free = %s, pnl = %s, fee = %s", mpd_to_sci(free, 0), mpd_to_sci(pnl, 0), mpd_to_sci(fee, 0));
                cancel = true;
                mpd_del(margin);
                mpd_del(total);
            }
        } else if (action < 3) {
            mpd_copy(total, margin, &mpd_ctx);
            mpd_add(total, margin, fee, &mpd_ctx);
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                if (action == 1) {
                    mpd_add(cur_margin, cur_margin, margin, &mpd_ctx);
                } else {
                    mpd_sub(cur_margin, cur_margin, margin, &mpd_ctx);
                }
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, pnl = %s, margin = %s, total = %s", mpd_to_sci(free, 0), mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                cancel = true;
                mpd_del(margin);
                mpd_del(total);
            }
        } else if (action == 3) {
            mpd_sub(total, update_margin, cur_margin, &mpd_ctx);
            mpd_add(total, total, fee, &mpd_ctx);
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);
                balance_sub_v2(sid, BALANCE_TYPE_MARGIN, cur_margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                mpd_sub(cur_margin, mpd_zero, update_margin, &mpd_ctx);
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, pnl = %s, margin = %s, total = %s", mpd_to_sci(free, 0), mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                cancel = true;
                mpd_del(margin);
                mpd_del(total);
            }
        } else {
            mpd_add(total, update_margin, cur_margin, &mpd_ctx);
            mpd_add(total, total, fee, &mpd_ctx);
            if (mpd_cmp(pnl, total, &mpd_ctx) > 0) {
                balance_add_v2(sid, BALANCE_TYPE_MARGIN, update_margin);
                balance_add_float(sid, BALANCE_TYPE_MARGIN, cur_margin);
                balance_sub_v2(sid, BALANCE_TYPE_EQUITY, fee);
                balance_sub_float(sid, BALANCE_TYPE_FREE, total);
                mpd_del(total);

                mpd_copy(cur_margin, update_margin, &mpd_ctx);
                market_set_margin(m, sid, cur_margin);

            } else {
                log_info("## free = %s, pnl = %s, margin = %s, total = %s", mpd_to_sci(free, 0), mpd_to_sci(pnl, 0), mpd_to_sci(margin, 0), mpd_to_sci(total, 0));
                cancel = true;
                mpd_del(margin);
                mpd_del(total);
            }
        }
    } else {
        log_info("## free = 0");
        cancel = true;
        mpd_del(margin);
    }

log_info("## cur_margin3 = %s", mpd_to_sci(cur_margin, 0));
    mpd_del(update_margin);

    // 撤单
    if (cancel) {
        mpd_copy(o->margin, mpd_zero, &mpd_ctx);
        mpd_copy(o->fee, mpd_zero, &mpd_ctx);
        mpd_copy(o->swap, mpd_zero, &mpd_ctx);
        const char* comment = "no enough money";
        o->comment = strdup(comment);
        o->finish_time = update_time;

        if (real) {
            int ret = finish_limit(o, true);
            if (ret < 0) {
                log_fatal("finish_limit fail: %d, order: %"PRIu64"", ret, o->id);
            }
            push_order_message(ORDER_EVENT_CANCEL, o, m);

            json_t *params = json_array();
            json_array_append_new(params, json_integer(sid));
            json_array_append_new(params, json_string(o->symbol));
            json_array_append_new(params, json_integer(o->id));
            json_array_append_new(params, json_string(comment));
            json_array_append_new(params, json_real(update_time));
            append_operlog("cancel_order", params);
        }

        order_cancel(m, o, true);
        return 0;
    }

    // 挂单成交
    order_cancel(m, o, false);

    o->type         = MARKET_ORDER_TYPE_LIMIT;
    o->update_time  = update_time;
    mpd_copy(o->price, price, &mpd_ctx);
    mpd_copy(o->margin, margin, &mpd_ctx);
    mpd_copy(o->margin_price, margin_price, &mpd_ctx);
    mpd_copy(o->profit_price, mpd_one, &mpd_ctx);
    mpd_del(margin);

    int ret = order_put_v2(m, o);
    if (ret < 0) {
        log_fatal("order_put fail: %d, order: %"PRIu64"", ret, o->id);
        return ret;
    }
    if (real) {
        ret = finish_limit(o, false);
        if (ret < 0) {
            log_fatal("finish_limit fail: %d, order: %"PRIu64"", ret, o->id);
        }
        ret = append_position(o);
        if (ret < 0) {
            log_fatal("append_position fail: %d, order: %"PRIu64"", ret, o->id);
        }
        push_order_message_v2(ORDER_EVENT_OPEN, o);

        json_t *params = json_array();
        json_array_append_new(params, json_integer(sid));
        json_array_append_new(params, json_string(o->symbol));
        json_array_append_new(params, json_integer(o->id));
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(price, 0))));
        json_array_append_new(params, json_string(rstripzero(mpd_to_sci(margin_price, 0))));
        json_array_append_new(params, json_real(update_time));
        append_operlog("limit_open", params);
    }

    return 0;
}

int limit_expire(order_t *order)
{
    market_t *m = get_market(order->symbol);

    mpd_copy(order->margin, mpd_zero, &mpd_ctx);
    mpd_copy(order->fee, mpd_zero, &mpd_ctx);
    mpd_copy(order->swap, mpd_zero, &mpd_ctx);
    order->comment = strdup("expire");
    order->finish_time = order->expire_time;

    int ret = finish_limit(order, true);
    if (ret < 0) {
        log_fatal("finish_limit fail: %d, order: %"PRIu64"", ret, order->id);
    }
    push_order_message(ORDER_EVENT_EXPIRE, order, m);

    json_t *params = json_array();
    json_array_append_new(params, json_integer(order->sid));
    json_array_append_new(params, json_string(order->symbol));
    json_array_append_new(params, json_integer(order->id));
    json_array_append_new(params, json_string(order->comment));
    json_array_append_new(params, json_real(order->finish_time));
    append_operlog("cancel_order", params);

    order_cancel(m, order, true);
    return 0;
}
