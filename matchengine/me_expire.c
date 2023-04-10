# include "me_expire.h"
# include "me_market.h"
# include "me_symbol.h"

static nw_timer timer;
static int flag = 0;

static void flush_list(void)
{
    if (expire_orders->len == 0)
        return;

    double time = current_timestamp();
    skiplist_node *node;
    skiplist_iter *iter = skiplist_get_iterator(expire_orders);
    while ((node = skiplist_next(iter)) != NULL) {
        order_t *order = node->value;
        if (order->expire_time > time)
            break;

        log_info("## [expire] %"PRIu64" %s %"PRIu64" - %"PRIu64" at %f", order->sid, order->symbol, order->id, order->expire_time, time);
        int ret = limit_expire(order);
        if (ret < 0) {
            log_fatal("limit expire fail: %d, order: %"PRIu64"", ret, order->id);
        }
    }
    skiplist_release_iterator(iter);
}

static void on_timer(nw_timer *t, void *privdata)
{
    if (flag == 0) {
        flag = 1;
        flush_list();
        flag = 0;
    }
}

int init_expire(void)
{
    nw_timer_set(&timer, 1.0, true, on_timer, NULL);
    nw_timer_start(&timer);
    return 0;
}
