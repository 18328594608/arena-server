# include "me_symbol.h"
# include "me_config.h"

struct configs configs;

static dict_t *dict_group;

struct group_type {
    int leverage;
};

static uint32_t group_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *group_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void *group_dict_val_dup(const void *val)
{
    struct group_type *obj = malloc(sizeof(struct group_type));
    if (obj == NULL)
        return NULL;
    memcpy(obj, val, sizeof(struct group_type));
    return obj;
}

static int group_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void group_dict_key_free(void *key)
{
    free(key);
}

static void group_dict_val_free(void *val)
{
    free(val);
}

static dict_t *dict_fee;

struct fee_type {
    mpd_t *percentage;
    mpd_t *fee;
    mpd_t *swap_long;
    mpd_t *swap_short;
};

static uint32_t fee_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *fee_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void *fee_dict_val_dup(const void *val)
{
    struct fee_type *obj = malloc(sizeof(struct fee_type));
    if (obj == NULL)
        return NULL;
    memcpy(obj, val, sizeof(struct fee_type));
    return obj;
}

static int fee_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void fee_dict_key_free(void *key)
{
    free(key);
}

static void fee_dict_val_free(void *val)
{
    struct fee_type *ft = val;
    mpd_del(ft->fee);
    mpd_del(ft->swap_long);
    mpd_del(ft->swap_short);
    free(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = group_dict_hash_function;
    type.key_compare    = group_dict_key_compare;
    type.key_dup        = group_dict_key_dup;
    type.key_destructor = group_dict_key_free;
    type.val_dup        = group_dict_val_dup;
    type.val_destructor = group_dict_val_free;

    dict_group = dict_create(&type, 64);
    if (dict_group == NULL)
        return -__LINE__;

    dict_types fee_dt;
    memset(&fee_dt, 0, sizeof(fee_dt));
    fee_dt.hash_function  = fee_dict_hash_function;
    fee_dt.key_compare    = fee_dict_key_compare;
    fee_dt.key_dup        = fee_dict_key_dup;
    fee_dt.key_destructor = fee_dict_key_free;
    fee_dt.val_dup        = fee_dict_val_dup;
    fee_dt.val_destructor = fee_dict_val_free;

    dict_fee = dict_create(&fee_dt, 64);
    if (dict_fee == NULL)
        return -__LINE__;

    return 0;
}

static int load_group_from_db(MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `group`, `leverage` FROM `group`");
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

    configs.group_num = num_rows;
    configs.groups = malloc(sizeof(struct group) * num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        configs.groups[i].name = strdup(row[0]);
        configs.groups[i].leverage = atoi(row[1]);
    }
    mysql_free_result(result);

    return 0;
}

static int load_symbol_from_db(MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `symbol`, `security`, `digit`, `currency`, `contract_size`, `percentage`, `margin_calc`, `profit_calc`, `swap_calc`, `tick_size`, `tick_price` FROM symbol");
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

    configs.symbol_num = num_rows;
    configs.symbols = malloc(sizeof(struct symbol) * num_rows);

    mpd_t *hundred = mpd_new(&mpd_ctx);
    mpd_set_string(hundred, "100", &mpd_ctx);

    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        const char *name = row[0];
        const char *currency = row[3];
        uint32_t margin_calc = atoi(row[6]);
        uint32_t profit_calc = atoi(row[7]);

        configs.symbols[i].name = strdup(name);
        configs.symbols[i].security = strdup(row[1]);
        configs.symbols[i].digit = atoi(row[2]);
        configs.symbols[i].currency = strdup(currency);
        configs.symbols[i].margin_calc = margin_calc;
        configs.symbols[i].profit_calc = profit_calc;
        configs.symbols[i].contract_size = decimal(row[4], PREC_INT);
        configs.symbols[i].c = decimal(row[4], PREC_INT);
        configs.symbols[i].percentage = decimal(row[5], PREC_INT);
        configs.symbols[i].swap_calc = atoi(row[8]);
        configs.symbols[i].tick_size = decimal(row[9], PREC_DEFAULT);
        configs.symbols[i].tick_price = decimal(row[10], PREC_DEFAULT);

        // c = contract_size / 100
        mpd_div(configs.symbols[i].c, configs.symbols[i].c, hundred, &mpd_ctx);

        if (margin_calc == MARGIN_CALC_FOREX) {
            if (strcmp(currency, "USD") == 0) {
                configs.symbols[i].margin_type = MARGIN_TYPE_UB; // USDJPY
            } else if (strcmp(currency, "JPY") == 0 || strcmp(currency, "HKD") == 0 || strcmp(currency, "CAD") == 0 || strcmp(currency, "CHF") == 0) {
                configs.symbols[i].margin_type = MARGIN_TYPE_BC; // CADJPY
                char ms[8] = { "USD" };
                configs.symbols[i].margin_symbol = strdup(strcat(ms, currency));
            } else if (strstr(name, "USD") != NULL) {
                configs.symbols[i].margin_type = MARGIN_TYPE_AU; // EURUSD
                configs.symbols[i].margin_symbol = strdup(name);
            } else {
                configs.symbols[i].margin_type = MARGIN_TYPE_AC; // EURGBP
                char ms[8] = { 0 };
                strcat(ms, currency);
                configs.symbols[i].margin_symbol = strdup(strcat(ms, "USD"));
            }
        } else {
            configs.symbols[i].margin_type = 0;
        }

        if (profit_calc == PROFIT_CALC_FOREX) {
            if (strcmp(currency, "USD") == 0) {
                configs.symbols[i].profit_type = PROFIT_TYPE_UB; // USDJPY
            } else {
                char quote[4] = {""};
                strncpy(quote, name + 3, 3);
                if (strcmp(quote, "USD") == 0) {
                    configs.symbols[i].profit_type = PROFIT_TYPE_AU; // EURUSD
                } else if (strcmp(quote, "JPY") == 0 || strcmp(quote, "HKD") == 0 || strcmp(quote, "CAD") == 0 || strcmp(quote, "CHF") == 0) {
                    configs.symbols[i].profit_type = PROFIT_TYPE_CB; // CADJPY
                    char ps[8] = { "USD" };
                    configs.symbols[i].profit_symbol = strdup(strcat(ps, quote));
                } else {
                    configs.symbols[i].profit_type = PROFIT_TYPE_AC; // EURGBP
                    char ps[8] = { 0 };
                    strcat(ps, quote);
                    configs.symbols[i].profit_symbol = strdup(strcat(ps, "USD"));
                }
            }
        } else {
            configs.symbols[i].profit_type = 0;
        }

    }

    mpd_del(hundred);
    mysql_free_result(result);

    return 0;
}

static int load_fee_from_db(MYSQL *conn)
{
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `symbol`, `group`, `percentage`, `fee`, `swap_long`, `swap_short` FROM fee");
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

    configs.fee_num = num_rows;
    configs.fees = malloc(sizeof(struct fee) * num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        configs.fees[i].symbol = strdup(row[0]);
        configs.fees[i].group = strdup(row[1]);
        configs.fees[i].percentage = decimal(row[2], PREC_INT);
        configs.fees[i].fee = decimal(row[3], PREC_DEFAULT);
        configs.fees[i].swap_long = decimal(row[4], PREC_SWAP);
        configs.fees[i].swap_short = decimal(row[5], PREC_SWAP);
    }
    mysql_free_result(result);

    return 0;
}

static struct group_type *get_group_type(const char *group)
{
    dict_entry *entry = dict_find(dict_group, group);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

symbol_t *get_symbol(const char *name)
{
    for (int i = 0; i < configs.symbol_num; ++i) {
        if (strcmp(name, configs.symbols[i].name) == 0)
            return &(configs.symbols[i]);
    }
    return NULL;
}

static struct fee_type *get_fee_type(const char *group, const char *symbol)
{
    char *key = (char *) malloc(strlen(group) + strlen(symbol) + 1);
    strcpy(key, group);
    strcat(key, symbol);
    dict_entry *entry = dict_find(dict_fee, key);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

int group_leverage(const char *group)
{
    struct group_type *at = get_group_type(group);
    return at ? at->leverage : 0;
}

mpd_t* symbol_percentage(const char *group, const char *symbol)
{
    struct fee_type *at = get_fee_type(group, symbol);
    return at ? at->percentage : mpd_one;
}

mpd_t* symbol_fee(const char *group, const char *symbol)
{
    struct fee_type *at = get_fee_type(group, symbol);
    return at ? at->fee : mpd_zero;
}

mpd_t* symbol_swap_long(const char *group, const char *symbol)
{
    struct fee_type *at = get_fee_type(group, symbol);
    return at ? at->swap_long : mpd_zero;
}

mpd_t* symbol_swap_short(const char *group, const char *symbol)
{
    struct fee_type *at = get_fee_type(group, symbol);
    return at ? at->swap_short : mpd_zero;
}

int init_symbol(void)
{
    ERR_RET(init_dict());

    MYSQL *conn = mysql_connect(&settings.db_config);
    if (conn == NULL) {
        log_error("connect mysql fail");
        log_stderr("connect mysql fail");
        return -__LINE__;
    }

    int ret = load_group_from_db(conn);
    if (ret < 0) {
        goto cleanup;
    }

    ret = load_symbol_from_db(conn);
    if (ret < 0) {
        goto cleanup;
    }

    ret = load_fee_from_db(conn);
    if (ret < 0) {
        goto cleanup;
    }

    mysql_close(conn);
    log_stderr("load symbol success");

    for (size_t i = 0; i < configs.group_num; ++i) {
        struct group_type gt;
        gt.leverage = configs.groups[i].leverage;
        if (dict_add(dict_group, configs.groups[i].name, &gt) == NULL)
            return -__LINE__;
    }

    for (size_t i = 0; i < configs.fee_num; ++i) {
        struct fee_type ft;
        ft.percentage = configs.fees[i].percentage;
        ft.fee = configs.fees[i].fee;
        ft.swap_long = configs.fees[i].swap_long;
        ft.swap_short = configs.fees[i].swap_short;
        char *group = configs.fees[i].group;
        char *symbol = configs.fees[i].symbol;
        char *key = (char *) malloc(strlen(group) + strlen(symbol) + 1);
        strcpy(key, group);
        strcat(key, symbol);
        if (dict_add(dict_fee, key, &ft) == NULL)
            return -__LINE__;
    }

    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}
