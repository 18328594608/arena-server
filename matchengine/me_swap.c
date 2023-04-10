# include "me_swap.h"
# include "me_market.h"
# include "me_balance.h"
# include "me_dump.h"
# include "me_tick.h"

static nw_timer timer;
static time_t last_swap_time;

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

static int get_days(time_t tt)
{
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    struct tm t;
    memset(&t, 0, sizeof(t));
    t.tm_year = lt->tm_year;
    t.tm_mon  = lt->tm_mon;
    t.tm_mday = lt->tm_mday;
    time_t cur = mktime(&t);

    lt = localtime(&tt);
    memset(&t, 0, sizeof(tt));
    t.tm_year = lt->tm_year;
    t.tm_mon  = lt->tm_mon;
    t.tm_mday = lt->tm_mday;
    time_t create = mktime(&t);

    if (cur < create)
        return 0;
    return (cur - create) / (3600 * 24);
}

static void make_swap(int wday)
{
    log_info("## swap job start ##");

    // 周六收取3倍
    uint32_t ex_days = 0;
    if (wday == 6)
       ex_days = 2;

    mpd_t *swaps = mpd_new(&mpd_ctx);
    mpd_t *delta = mpd_new(&mpd_ctx); // 两次隔夜费的差值
    mpd_t *days_t = mpd_new(&mpd_ctx);

    for (int i = 0; i < configs.symbol_num; ++i) {
        market_t *m = get_market(configs.symbols[i].name);
        symbol_t *sym = get_symbol(configs.symbols[i].name);

        skiplist_node *node;
        skiplist_iter *iter = skiplist_get_iterator(m->buys);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            time_t ct = order->update_time > 0 ? (time_t) order->update_time : (time_t) order->create_time;
            uint32_t days = get_days(ct) + ex_days;
            if (days > 0) {
                mpd_set_u32(days_t, days, &mpd_ctx);
                mpd_mul(swaps, order->swap, days_t, &mpd_ctx);
                mpd_mul(swaps, swaps, order->lot, &mpd_ctx);

                if (sym->margin_calc == MARGIN_CALC_FOREX) {
                    if (sym->margin_type == MARGIN_TYPE_AU) {
                        mpd_mul(swaps, swaps, order->price, &mpd_ctx);        // EURUSD
                    } else if (sym->margin_type == MARGIN_TYPE_AC) {
                        mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // EURGBP
                    } else if (sym->margin_type == MARGIN_TYPE_BC) {
                        mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // CADJBP
                    }
                } else if (strcmp(sym->name, "HSI") == 0) {
                    mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // USDHKD
                } else if (strcmp(sym->name, "DAX") == 0) {
                    mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // EURUSD
                } else if (strcmp(sym->name, "UK100") == 0) {
                    mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // GBPUSD
                } else if (strcmp(sym->name, "JP225") == 0) {
                    mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // USDJPY
                }

                mpd_rescale(swaps, swaps, -2, &mpd_ctx);
                mpd_sub(delta, swaps, order->swaps, &mpd_ctx);
                mpd_copy(order->swaps, swaps, &mpd_ctx);

log_info("## [%"PRIu64"] buy [%s] swaps = %s", order->id, sym->name, mpd_to_sci(swaps, 0));

                balance_sub_v2(order->sid, BALANCE_TYPE_EQUITY, delta);
                balance_sub_v2(order->sid, BALANCE_TYPE_FREE, delta);
            }
        }
        skiplist_release_iterator(iter);

        iter = skiplist_get_iterator(m->sells);
        while ((node = skiplist_next(iter)) != NULL) {
            order_t *order = node->value;
            time_t ct = order->update_time > 0 ? (time_t) order->update_time : (time_t) order->create_time;
            uint32_t days = get_days(ct) + ex_days;
            if (days > 0) {
                mpd_set_u32(days_t, days, &mpd_ctx);
                mpd_mul(swaps, order->swap, days_t, &mpd_ctx);
                mpd_mul(swaps, swaps, order->lot, &mpd_ctx);

                if (sym->margin_calc == MARGIN_CALC_FOREX) {
                    if (sym->margin_type == MARGIN_TYPE_AU) {
                        mpd_mul(swaps, swaps, order->price, &mpd_ctx);        // EURUSD
                    } else if (sym->margin_type == MARGIN_TYPE_AC) {
                        mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // EURGBP
                    } else if (sym->margin_type == MARGIN_TYPE_BC) {
                        mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // CADJBP
                    }
                } else if (strcmp(sym->name, "HSI") == 0) {
                    mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // USDHKD
                } else if (strcmp(sym->name, "DAX") == 0) {
                    mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // EURUSD
                } else if (strcmp(sym->name, "UK100") == 0) {
                    mpd_mul(swaps, swaps, order->margin_price, &mpd_ctx); // GBPUSD
                } else if (strcmp(sym->name, "JP225") == 0) {
                    mpd_div(swaps, swaps, order->margin_price, &mpd_ctx); // USDJPY
                }

                mpd_rescale(swaps, swaps, -2, &mpd_ctx);
                mpd_sub(delta, swaps, order->swaps, &mpd_ctx);
                mpd_copy(order->swaps, swaps, &mpd_ctx);

log_info("## [%"PRIu64"] sell [%s] swaps = %s", order->id, sym->name, mpd_to_sci(swaps, 0));

                balance_sub_v2(order->sid, BALANCE_TYPE_EQUITY, delta);
                balance_sub_v2(order->sid, BALANCE_TYPE_FREE, delta);
            }
        }
        skiplist_release_iterator(iter);
    }

    last_swap_time += 3600 * 24;

    mpd_del(swaps);
    mpd_del(delta);
    mpd_del(days_t);

    log_info("## swap job end ##");
}

static void on_timer(nw_timer *timer, void *privdata)
{
    time_t now = time(NULL);
    if (now - last_swap_time < 3600 * 24)
        return;

    struct tm *local = localtime(&now);
//    struct tm *gt = gmtime(&now);
    // 周日 (wday = 0) 和周一无需计算
    if (local->tm_wday < 2)
        return;

    make_swap(local->tm_wday);

    make_slice(now);
}

int init_swap(void)
{
    last_swap_time = get_today_start();

    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);

    return 0;
}

