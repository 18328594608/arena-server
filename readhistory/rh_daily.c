# include "rh_config.h"
# include "rh_daily.h"

static nw_timer timer;
static time_t last_daily_time;

static time_t get_today_start(void)
{
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    struct tm t;
    memset(&t, 0, sizeof(t));
    t.tm_year = lt->tm_year;
    t.tm_mon  = lt->tm_mon;
    t.tm_mday = lt->tm_mday;
    return mktime(&t);
}

static int make_daily()
{
    MYSQL *conn = mysql_connect(&settings.db_log);
    if (conn == NULL) {
        log_error("connect mysql log fail");
        return -__LINE__;
    }

    time_t now = get_today_start();
    sds sql = sdsempty();
    sql = sdscatprintf(sql, "SELECT `sid`, `balance` FROM slice_balance_%ld WHERE t=1", now);

    log_trace("exec sql: %s", sql);
    int ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(sql);
        mysql_close(conn);
        return -__LINE__;
    }

    MYSQL_RES *result = mysql_store_result(conn);
    size_t num_rows = mysql_num_rows(result);
    json_t *balances = json_object();
    for (int i = 0; i < num_rows; ++i) {
        MYSQL_ROW row = mysql_fetch_row(result);
        json_object_set_new(balances, row[0], json_string(rstripzero(row[1]))); 
    }

    mysql_free_result(result);
    mysql_close(conn);

    conn = mysql_connect(&settings.db_history);
    if (conn == NULL) {
        sdsfree(sql);
        json_decref(balances);
        log_error("connect mysql history fail");
        return -__LINE__;
    }

    struct tm *tm = localtime(&now);
    sds table = sdsempty();
    table = sdscatprintf(table, "balance_daily_%04d%02d%02d", 1900 + tm->tm_year, 1 + tm->tm_mon, tm->tm_mday);
   
    sql = sdsempty();
    sql = sdscatprintf(sql, "CREATE TABLE IF NOT EXISTS `%s` like `balance_daily_example`", table); 

    ret = mysql_real_query(conn, sql, sdslen(sql));
    if (ret != 0 && mysql_errno(conn) != 1062) {
        log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
        sdsfree(table);
        sdsfree(sql);
        json_decref(balances);
        mysql_close(conn);
        return -__LINE__;
    }

    for (size_t i = 0; i < 100; ++i) {
        sql = sdsempty();
        sql = sdscatprintf(sql, "SELECT `sid`, SUM( case when `business`=1 then `change` else 0 end ) `deposit`, "
                "SUM( case when `business`=2 then `change` else 0 end ) `profit` FROM `balance_history_%d` WHERE "
                "`time` >= %"PRIu64" AND `time` < %"PRIu64" GROUP BY `sid`", i, now - 3600 * 24, now);

        log_trace("exec sql: %s", sql);
        ret = mysql_real_query(conn, sql, sdslen(sql));
        if (ret != 0) {
            log_fatal("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
            sdsfree(table);
            sdsfree(sql);
            json_decref(balances);
            mysql_close(conn);
            return -__LINE__;
        }

        size_t insert_limit = 1000;
        size_t index = 0;
        sdsclear(sql);
       
        MYSQL_RES *result = mysql_store_result(conn);
        size_t num_rows = mysql_num_rows(result);
        for (int i = 0; i < num_rows; ++i) {
            MYSQL_ROW row = mysql_fetch_row(result);
            uint64_t sid = strtoull(row[0], NULL, 0);
            
            if (index == 0) {
                sql = sdscatprintf(sql, "INSERT INTO `%s` (`id`, `sid`, `deposit`, `profit`, `balance`) VALUES ", table);
            } else {
                sql = sdscatprintf(sql, ", ");
            }

            json_t *node = json_object_get(balances, row[0]);
            if (!node) {
                sql = sdscatprintf(sql, "(NULL, %"PRIu64", '%s', '%s', '0')", sid, row[1], row[2]);
            } else {
                sql = sdscatprintf(sql, "(NULL, %"PRIu64", '%s', '%s', '%s')", sid, row[1], row[2], json_string_value(json_object_get(balances, row[0])));
            }

            index += 1;
            if (index == insert_limit) {
                log_trace("exec sql: %s", sql);
                int ret = mysql_real_query(conn, sql, sdslen(sql));
                if (ret < 0) {
                    log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                    sdsfree(table);
                    sdsfree(sql);
                    json_decref(balances);
                    mysql_close(conn);
                    return -__LINE__;
                }

                sdsclear(sql);
                index = 0;
            }
        }

        if (index > 0) {
            log_trace("exec sql: %s", sql);
            int ret = mysql_real_query(conn, sql, sdslen(sql));
            if (ret < 0) {
                log_error("exec sql: %s fail: %d %s", sql, mysql_errno(conn), mysql_error(conn));
                sdsfree(table);
                sdsfree(sql);
                json_decref(balances);
                mysql_close(conn);
                return -__LINE__;
            }
        }

        mysql_free_result(result);
    }

    sdsfree(table);
    sdsfree(sql);
    json_decref(balances);
    mysql_close(conn);

    last_daily_time += 3600 * 24;

    return 0;
}

static void on_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if (now - last_daily_time < 3600 * 24)
        return;

    log_info("## daily job start ##");
    int ret = make_daily();
    if (ret < 0) {
        log_error("make_daily fail: %d", ret);
    }
    log_info("## daily job end ##");
}

int init_daily(void)
{
    // 延迟执行，等隔夜费和订单备份完成
    last_daily_time = get_today_start() + settings.daily_time;

    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

