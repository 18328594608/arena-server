# include "me_tick.h"
# include "me_symbol.h"
# include "me_config.h"
//# include "me_limit.h"

//struct configs configs;
static dict_t *dict_tick;
int status;

struct tick_type {
    mpd_t *bid;
    mpd_t *ask;
};

static uint32_t tick_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *tick_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void *tick_dict_val_dup(const void *val)
{
    struct tick_type *obj = malloc(sizeof(struct tick_type));
    if (obj == NULL)
        return NULL;
    memcpy(obj, val, sizeof(struct tick_type));
    return obj;
}

static int tick_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void tick_dict_key_free(void *key)
{
    free(key);
}

static void tick_dict_val_free(void *val)
{
    struct tick_type *tt = val;
    mpd_del(tt->bid);
    mpd_del(tt->ask);
    free(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = tick_dict_hash_function;
    type.key_compare    = tick_dict_key_compare;
    type.key_dup        = tick_dict_key_dup;
    type.key_destructor = tick_dict_key_free;
    type.val_dup        = tick_dict_val_dup;
    type.val_destructor = tick_dict_val_free;

    dict_tick = dict_create(&type, 64);
    if (dict_tick == NULL)
        return -__LINE__;

    for (int i = 0; i < configs.symbol_num; ++i) {
        struct tick_type tt;
        tt.bid = mpd_new(&mpd_ctx);
        tt.ask = mpd_new(&mpd_ctx);
        // USDHKD 行情固定
        if (strcmp(configs.symbols[i].name, "USDHKD") == 0) {
            mpd_set_string(tt.bid, "7.843", &mpd_ctx);
            mpd_set_string(tt.ask, "7.844", &mpd_ctx);
        } else {
            mpd_copy(tt.bid, mpd_zero, &mpd_ctx);
            mpd_copy(tt.ask, mpd_zero, &mpd_ctx);
        }
        if (dict_add(dict_tick, configs.symbols[i].name, &tt) == NULL)
            return -__LINE__; 
    }

    return 0;
}

static struct tick_type *get_tick_type(const char *symbol)
{
    dict_entry *entry = dict_find(dict_tick, symbol);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

mpd_t* symbol_bid(const char *symbol)
{
    struct tick_type *at = get_tick_type(symbol);
    if (at) {
        return at->bid;
    }
    return mpd_zero;
}

mpd_t* symbol_ask(const char *symbol)
{
    struct tick_type *at = get_tick_type(symbol);
    if (at) {
        return at->ask;
    }
    return mpd_zero;
}

/*
static void tick_update(char *msg)
{
    const char *delim = ",";
    char *bid;
    char *ask;

    char *p = strtok(msg, delim);
    char *symbol = strdup(p);
    size_t idx = 0;
    while((p = strtok(NULL, delim))) {
        if (idx == 0)
            bid = strdup(p);
        else if (idx == 1)
            ask = strdup(p);
        else
            break;
        idx++;
    }

    struct tick_type *at = get_tick_type(symbol);
    if (at) {
        strcpy(at->bid, bid);
        strcpy(at->ask, ask);
    }

    free(symbol);
    free(ask);
    free(bid);
}
*/

static void on_open(struct uwsc_client *cl)
{
    log_info("[ws] on_open");

    char *parm = "qwertyuiopasdfghjklzxcvbnm123456{\"cmd\": \"intervalTicks\",\"data\": {\"symbol\": [\"ADAUSD.hb\",\"AUDCAD\",\"AUDCHF\",\"AUDJPY\","
                 "\"AUDNZD\",\"AUDUSD\",\"AXSUSD.hb\",\"BCHUSD.hb\",\"BNBUSD.hb\",\"BTCUSD\",\"CADCHF\",\"CADJPY\",\"CHFJPY\",\"CL\",\"DAX\","
                 "\"DOGEUSD.hb\",\"EOSUSD.hb\",\"ETCUSD\",\"ETHUSD\",\"EURAUD\",\"EURCAD\",\"EURCHF\",\"EURGBP\",\"EURJPY\",\"EURNZD\",\"EURUSD\","
                 "\"GBPAUD\",\"GBPCAD\",\"GBPCHF\",\"GBPJPY\",\"GBPNZD\",\"GBPUSD\",\"HSI\",\"INDIA50\",\"LINKUSD.hb\",\"LTCUSD\",\"MANAUSD.hb\","
                 "\"NQ\",\"NZDCAD\",\"NZDCHF\",\"NZDJPY\",\"NZDUSD\",\"TRXUSD.hb\",\"US30\",\"US500\",\"USDCAD\",\"USDCHF\",\"USDJPY\","
                 "\"WRXUSD.hb\",\"XAGUSD\",\"XAUUSD\",\"XRPUSD\",\"DOGEUSDT\",\"XRPUSDT\",\"EOSUSDT\",\"TRXUSDT\"]}}";
    cl->send_ex(cl, UWSC_OP_TEXT, 1, strlen(parm), parm);
}

static void on_message(struct uwsc_client *cl, void *data, size_t len, bool binary)
{
    if (binary) {
        size_t i;
        uint8_t *p = data;
        for (i = 0; i < len; i++) {
            printf("%02hhX ", p[i]);
            if (i % 16 == 0 && i > 0)
                puts("");
        }
        puts("");
    } else {

// log_info("[%.*s]", (int)len, (char *)data);
// qwertyuiopasdfghjklzxcvbnm123456{"cmd":"intervalTicks","code":0,"comment":"","data":[{"a":"1488.350000","b":"1487.380000","g":6,"s":"ETHUSD","t":1658384901}],"message":""}
// 00000000000000000000000000000000{"cmd":"intervalTicks","data":[{"a":"1488.390000","b":"1487.560000","g":6,"s":"ETHUSD","t":1658384906}]}

        // MT4 tick
        sds msg = sdsempty();
        msg = sdscatlen(msg, data + 32, len - 32);

        json_error_t error;
        json_t *root = json_loads(msg, 0, &error);
        json_t *node = json_object_get(root, "data");
        if (!node || !json_is_array(node))
            return;

        json_t *entry = json_array_get(node, 0);
        json_t *s = json_object_get(entry, "s");
        json_t *a = json_object_get(entry, "a");
        json_t *b = json_object_get(entry, "b");
        json_t *t = json_object_get(entry, "t");
//log_info("## %s = %s / %s", json_string_value(s), json_string_value(b), json_string_value(a));

        char *symbol = strdup(json_string_value(s));
        log_tick("[%s] %s / %s (%d)", symbol, json_string_value(b), json_string_value(a), json_integer_value(t));

        struct tick_type *at = get_tick_type(symbol);
        if (at) {
            mpd_set_string(at->bid, json_string_value(b), &mpd_ctx);
            mpd_set_string(at->ask, json_string_value(a), &mpd_ctx);
        }

//        append_tpsl(symbol);
//        append_limit_symbol(symbol);

        json_decref(root);
        free(symbol);
        sdsfree(msg);

/*
        // AB-Hub tick
        if (len < 21)
            return;

        char msg[len + 1];
        strncpy(msg, (char *)data, len);
        tick_update(msg);
*/
    }
}

static void on_error(struct uwsc_client *cl, int err, const char *msg)
{
    status = 0;
    log_error("[ws] on_error:%d, %s", err, msg);
    sleep(2);
    reconnect(cl);
}

static void on_close(struct uwsc_client *cl, int code, const char *reason)
{
    status = 0;
    log_error("[ws] on_close:%d, %s", code, reason);
    sleep(2);
    reconnect(cl);
}

static void on_open_fix(struct uwsc_client *cl)
{
    status = 1;
    log_info("[ws] on_open_fix");
}

static void on_message_fix(struct uwsc_client *cl, void *data, size_t len, bool binary)
{
    if (binary) {
        size_t i;
        uint8_t *p = data;
        for (i = 0; i < len; i++) {
            printf("%02hhX ", p[i]);
            if (i % 16 == 0 && i > 0)
                puts("");
        }
        puts("");
    } else {

// [{"s":"NZDCAD","b":"0.87445","a":"0.8746","t":"1670837030917"},{"s":"EOSUSDT","b":"0.9775","a":"0.98","t":"1670837031721"},{"s":"EURNZD","b":"1.64782","a":"1.64804","t":"1670837031012"}]
// {"s":"BTCUSD","b":"16938.4","a":"16948.72","t":"1670837032014"}

        // 过滤[]的情况
        if (len < 10)
            return;

        // Centroid tick
        sds msg = sdsempty();
        msg = sdscatlen(msg, data, len);

        json_error_t error;
        json_t *root = json_loads(msg, 0, &error);
        if (!root)
            return;

        // 第一次收到的消息是数组
        if (len > 200) {
            for (size_t i = 0; i < json_array_size(root); ++i) {
                json_t *entry = json_array_get(root, i);
                json_t *s = json_object_get(entry, "s");
                json_t *a = json_object_get(entry, "a");
                json_t *b = json_object_get(entry, "b");
                json_t *t = json_object_get(entry, "t");

                char *symbol = strdup(json_string_value(s));
                log_tick("[%s] %s / %s (%s)", symbol, json_string_value(b), json_string_value(a), json_string_value(t));

                struct tick_type *at = get_tick_type(symbol);
                if (at) {
                    mpd_set_string(at->bid, json_string_value(b), &mpd_ctx);
                    mpd_set_string(at->ask, json_string_value(a), &mpd_ctx);
                }

                append_tpsl(symbol);
                append_stop_symbol(symbol);
//                append_limit_symbol(symbol);
                free(symbol);
            }
        } else {
            json_t *s = json_object_get(root, "s");
            json_t *a = json_object_get(root, "a");
            json_t *b = json_object_get(root, "b");
            json_t *t = json_object_get(root, "t");

            char *symbol = strdup(json_string_value(s));
            log_tick("[%s] %s / %s (%s)", symbol, json_string_value(b), json_string_value(a), json_string_value(t));

            struct tick_type *at = get_tick_type(symbol);
            if (at) {
                mpd_set_string(at->bid, json_string_value(b), &mpd_ctx);
                mpd_set_string(at->ask, json_string_value(a), &mpd_ctx);
            }

            append_tpsl(symbol);
            append_stop_symbol(symbol);
//            append_limit_symbol(symbol);
            free(symbol);
        }

        json_decref(root);
        sdsfree(msg);
    }

}

void reconnect(struct uwsc_client *cli)
{
    void (*open)(struct uwsc_client) = &on_open_fix;
    void (*message)(struct uwsc_client, void *, size_t, bool) = &on_message_fix;
    void (*error)(struct uwsc_client, int, const char*) = &on_error;
    void (*close)(struct uwsc_client, int, const char*) = &on_close;

    cli = ws_cli_create(settings.tick_svr, open, message, error, close);
    if (cli == NULL)
        log_error("[ws] reconnect failed");
}

int tick_status()
{
    return status;
}

int init_tick(void)
{
    status = 0;
    ERR_RET(init_dict());

    void (*open)(struct uwsc_client) = &on_open_fix;
    void (*message)(struct uwsc_client, void *, size_t, bool) = &on_message_fix;
    void (*error)(struct uwsc_client, int, const char*) = &on_error;
    void (*close)(struct uwsc_client, int, const char*) = &on_close;

    struct uwsc_client *cli = ws_cli_create(settings.tick_svr, open, message, error, close);
    if (cli == NULL)
        return -__LINE__;

    return 0;
}

