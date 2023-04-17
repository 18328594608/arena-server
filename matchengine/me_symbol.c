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

static dict_t *dict_week;

struct week_type {
    char            *monday;
    char            *tuesday;
    char            *wednesday;
    char            *thursday;
    char            *friday;
};

static uint32_t week_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, strlen(key));
}

static void *week_dict_key_dup(const void *key)
{
    return strdup(key);
}

static void *week_dict_val_dup(const void *val)
{
    struct week_type *obj = malloc(sizeof(struct week_type));
    if (obj == NULL)
        return NULL;
    memcpy(obj, val, sizeof(struct week_type));
    return obj;
}

static int week_dict_key_compare(const void *key1, const void *key2)
{
    return strcmp(key1, key2);
}

static void week_dict_key_free(void *key)
{
    free(key);
}

static void week_dict_val_free(void *val)
{
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

    dict_types week_dt;
    memset(&week_dt, 0, sizeof(week_dt));
    week_dt.hash_function  = week_dict_hash_function;
    week_dt.key_compare    = week_dict_key_compare;
    week_dt.key_dup        = week_dict_key_dup;
    week_dt.key_destructor = week_dict_key_free;
    week_dt.val_dup        = week_dict_val_dup;
    week_dt.val_destructor = week_dict_val_free;

    dict_week = dict_create(&week_dt, 64);
    if (dict_week == NULL)
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
    sql = sdscatprintf(sql,"SELECT `symbol`, `security`, `digit`, `currency`, `contract_size`, `percentage`, `margin_calc`, `profit_calc`, `swap_calc`, `tick_size`, `tick_price` , `monday`, `tuesday`, `wednesday`, `thursday`, `friday` FROM symbol");
    //sql = sdscatprintf(sql, "SELECT `symbol`, `security`, `digit`, `currency`, `contract_size`, `percentage`, `margin_calc`, `profit_calc`, `swap_calc`, `tick_size`, `tick_price` FROM symbol");

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

        const char *monday = row[11];
        const char *tuesday = row[12];
        const char *wednesday = row[13];
        const char *thursday = row[14];
        const char *friday = row[15];

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
        configs.symbols[i].monday = strdup(monday);
        configs.symbols[i].tuesday = strdup(tuesday);
        configs.symbols[i].wednesday = strdup(wednesday);
        configs.symbols[i].thursday = strdup(thursday);
        configs.symbols[i].friday = strdup(friday);
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

static struct week_type *get_week_type(const char *symbol)
{
    dict_entry *entry = dict_find(dict_week, symbol);
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

const char* week_monday(const char *symbol)
{
    struct week_type *at = get_week_type(symbol);
    return at ? at->monday : "";
}

const char* week_tuesday(const char *symbol)
{
    struct week_type *at = get_week_type(symbol);
    return at ? at->tuesday : "";
}

const char* week_wednesday(const char *symbol)
{
    struct week_type *at = get_week_type(symbol);
    return at ? at->wednesday : "";
}
const char* week_thursday(const char *symbol)
{
    struct week_type *at = get_week_type(symbol);
    return at ? at->thursday : "";
}

const char* week_friday(const char *symbol)
{
    struct week_type *at = get_week_type(symbol);
    return at ? at->friday : "";
}

int get_weekday(int timezone_offset) {
    time_t current_time;
    struct tm *tm_info;
    time(&current_time);

    // 获取UTC时间并加上时区偏移量
    time_t utc_time = current_time + (8 - timezone_offset) * 3600;

    tm_info = gmtime(&utc_time);
    // 转换为本地时间并获取当前是星期几
    time_t local_time = mktime(tm_info);
    tm_info = localtime(&local_time);
    return tm_info->tm_wday;
}

bool check_time_in_range(const char *time_range, int timezone_offset) {
    if (strlen(time_range) == 0)
    {
        return true;
    }

    char range_copy[strlen(time_range) + 1];
    strcpy(range_copy, time_range);

    char *range_parts[20];
    int range_count = 0;

    // 按 '|' 字符分隔时间区间
    char *range_part = strtok(range_copy, "|");
    while (range_part != NULL && range_count < 20) {
        range_parts[range_count++] = range_part;
        range_part = strtok(NULL, "|");
    }

    // 检查当前时间是否在任何时间区间内y
    time_t current_time = time(NULL) - (8 - timezone_offset) * 3600;;
    struct tm *tm = localtime(&current_time);
    char time_str[6];
    strftime(time_str, sizeof(time_str), "%H:%M", tm);

    for (int i = 0; i < range_count; i++) {
        if (is_time_in_range(time_str, range_parts[i])) {
            return true;
        }
    }

    return false;
}

// 检查给定时间是否在给定的时间范围内
 bool is_time_in_range(const char *time_str, const char *range_str) {
    int start_hour, start_minute, end_hour, end_minute;
    if (sscanf(range_str, "%d:%d-%d:%d", &start_hour, &start_minute, &end_hour, &end_minute) != 4) {
        return false;
    }

    int time_hour, time_minute;
    if (sscanf(time_str, "%d:%d", &time_hour, &time_minute) != 2) {
        return false;
    }

    int start_time = start_hour * 60 + start_minute;
    int end_time = end_hour * 60 + end_minute;
    int time = time_hour * 60 + time_minute;

    if (end_time < start_time) {
        // 时间范围跨越了午夜，需要调整结束时间和当前时间
        end_time += 24 * 60;
        if (time < start_time) {
            time += 24 * 60;
        }
    }

    return time >= start_time && time <= end_time;
}

bool symbol_check_time_in_range(const char *symbol_str)
{
    bool time_in_range = false;
    int week = get_weekday(settings.gmt_time);
    switch (week) {
        case 1: {
            const char *monday = week_monday(symbol_str);
            time_in_range = check_time_in_range(monday, settings.gmt_time);
            break;
        }
        case 2: {
            const char *tuesday = week_thursday(symbol_str);
            time_in_range = check_time_in_range(tuesday, settings.gmt_time);
        }
        case 3: {
            const char *wednesday = week_wednesday(symbol_str);
            time_in_range = check_time_in_range(wednesday, settings.gmt_time);
            break;
        }
        case 4: {
            const char *thursday = week_thursday(symbol_str);
            time_in_range = check_time_in_range(thursday, settings.gmt_time);
            break;
        }
        case 5: {
            const char *friday = week_friday(symbol_str);
            time_in_range = check_time_in_range(friday, settings.gmt_time);
            break;
        }
        default: {
            const char *monday = week_monday(symbol_str);
            const char *tuesday = week_thursday(symbol_str);
            const char *wednesday = week_wednesday(symbol_str);
            const char *thursday = week_thursday(symbol_str);
            const char *friday = week_friday(symbol_str);
            if (strlen(monday) == 0 && strlen(tuesday) == 0 && strlen(wednesday) == 0
                && strlen(thursday) == 0 && strlen(friday) == 0) {
                time_in_range = true;
            } else {
                time_in_range = false;
            }
            break;
        }
    }
    return time_in_range;
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
    for (int i = 0; i < configs.symbol_num; ++i) {
        struct week_type wt;
        wt.monday = malloc(strlen(configs.symbols[i].monday) + 1);
        wt.tuesday = malloc(strlen(configs.symbols[i].tuesday) + 1);
        wt.wednesday = malloc(strlen(configs.symbols[i].wednesday) + 1);
        wt.thursday = malloc(strlen(configs.symbols[i].thursday) + 1);
        wt.friday = malloc(strlen(configs.symbols[i].friday) + 1);

        if (wt.monday == NULL || wt.tuesday == NULL ||
            wt.wednesday == NULL || wt.thursday == NULL || wt.friday == NULL) {
            exit(1);
        }

        strcpy(wt.monday, configs.symbols[i].monday);
        strcpy(wt.tuesday, configs.symbols[i].tuesday);
        strcpy(wt.wednesday, configs.symbols[i].wednesday);
        strcpy(wt.thursday, configs.symbols[i].thursday);
        strcpy(wt.friday, configs.symbols[i].friday);
        if (dict_add(dict_week, configs.symbols[i].name, &wt) == NULL)
            return -__LINE__;
    }
    return 0;

cleanup:
    mysql_close(conn);
    return ret;
}
