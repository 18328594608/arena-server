# include "me_config.h"
# include "me_symbol.h"
# include "me_trade.h"

static dict_t *dict_market;

static uint32_t market_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static int market_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void *market_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void market_dict_key_free(void *key)
{
    free(key);
}

int init_trade(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = market_dict_hash_function;
    type.key_compare = market_dict_key_compare;
    type.key_dup = market_dict_key_dup;
    type.key_destructor = market_dict_key_free;

    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

/*
    for (size_t i = 0; i < settings.market_num; ++i) {
        market_t *m = market_create(&settings.markets[i]);
        if (m == NULL) {
            return -__LINE__;
        }

        dict_add(dict_market, settings.markets[i].name, m);
    }
*/

    return 0;
}

// 挂单过期按时间从小到大排序，相同值按订单号排序
static int order_expire_compare(const void *value1, const void *value2)
{
    const order_t *order1 = value1;
    const order_t *order2 = value2;

    if (order1->id == order2->id) {
        return 0;
    }

    int cmp = mpd_cmp(order1->expire_time, order2->expire_time, &mpd_ctx);
    if (cmp != 0) {
        return cmp;
    }
    return order1->id > order2->id ? 1 : -1;
}

int init_trade_v2(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function = market_dict_hash_function;
    type.key_compare = market_dict_key_compare;
    type.key_dup = market_dict_key_dup;
    type.key_destructor = market_dict_key_free;

    dict_market = dict_create(&type, 64);
    if (dict_market == NULL)
        return -__LINE__;

    for (size_t i = 0; i < configs.symbol_num; ++i) {
        market_t *m = market_create_v2(&configs.symbols[i]);
        if (m == NULL) {
            return -__LINE__;
        }

        dict_add(dict_market, configs.symbols[i].name, m);
    }

    skiplist_type st;
    memset(&st, 0, sizeof(st));
    st.compare = order_expire_compare;
    expire_orders = skiplist_create(&st);
    if (expire_orders == NULL)
        return -__LINE__;

    return 0;
}

market_t *get_market(const char *name)
{
    dict_entry *entry = dict_find(dict_market, name);
    if (entry)
        return entry->val;
    return NULL;
}

