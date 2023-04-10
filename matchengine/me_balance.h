# ifndef _ME_BALANCE_H_
# define _ME_BALANCE_H_

/**
 * Equity       = Balance + PNL - fees - swaps
 * Margin Free  = Equity - Margin
 * Margin Level = Equity / Margin * 100%
 */
# define BALANCE_TYPE_BALANCE   1
# define BALANCE_TYPE_EQUITY    2
# define BALANCE_TYPE_MARGIN    3
# define BALANCE_TYPE_FREE      4
# define BALANCE_TYPE_FLOAT     5

# define BUSINESS_TYPE_UPDATE   1
# define BUSINESS_TYPE_TRADE    2

# define BALANCE_TYPE_AVAILABLE 11
# define BALANCE_TYPE_FREEZE    12

extern dict_t *dict_balance;

struct balance_key {
    uint32_t    user_id;
    uint32_t    sid1; // sid1 = sid / 100
    uint32_t    sid2; // sid2 = sid % 100
    uint32_t    type;
};

int init_balance(void);

mpd_t *balance_get(uint32_t user_id, uint32_t type);
void   balance_del(uint32_t user_id, uint32_t type);
mpd_t *balance_set(uint32_t user_id, uint32_t type, mpd_t *amount);
mpd_t *balance_add(uint32_t user_id, uint32_t type, mpd_t *amount);
mpd_t *balance_sub(uint32_t user_id, uint32_t type, mpd_t *amount);
mpd_t *balance_freeze(uint32_t user_id, mpd_t *amount);
mpd_t *balance_unfreeze(uint32_t user_id, mpd_t *amount);

mpd_t *balance_total(uint32_t user_id);
int balance_status(mpd_t *total, size_t *available_count, mpd_t *available, size_t *freeze_count, mpd_t *freeze);

mpd_t *balance_get_v2(uint64_t sid, uint32_t type);
void   balance_del_v2(uint64_t sid, uint32_t type);
mpd_t *balance_set_v2(uint64_t sid, uint32_t type, mpd_t *amount);
mpd_t *balance_add_v2(uint64_t sid, uint32_t type, mpd_t *amount);
mpd_t *balance_sub_v2(uint64_t sid, uint32_t type, mpd_t *amount);

mpd_t *balance_get_float(uint64_t sid, uint32_t type);
mpd_t *balance_set_float(uint64_t sid, uint32_t type, mpd_t *amount);
mpd_t *balance_add_float(uint64_t sid, uint32_t type, mpd_t *amount);
mpd_t *balance_sub_float(uint64_t sid, uint32_t type, mpd_t *amount);

# endif

