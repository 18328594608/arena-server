# ifndef _ME_SYMBOL_H_
# define _ME_SYMBOL_H_

#include <stdbool.h>
# include "ut_decimal.h"

// Forex = lots * contract_size / leverage * percentage / 100
// CFD   = lots * contract_size / leverage * percentage / 100 * market_price
# define MARGIN_CALC_FOREX      1
# define MARGIN_CALC_CFD        2

// AU = EURUSD
// UB = USDJPY
// AC = EURGBP
// BC = CADJPY
# define MARGIN_TYPE_AU         1
# define MARGIN_TYPE_UB         2
# define MARGIN_TYPE_AC         3
# define MARGIN_TYPE_BC         4

// Forex / CFD = (close_price - open_price) * contract_size * lots
// Futures = (close_price - open_price) * tick_price / tick_size * lots
# define PROFIT_CALC_FOREX      1
# define PROFIT_CALC_CFD        2
# define PROFIT_CALC_FUTURES    3

// AU = EURUSD
// UB = USDJPY
// AC = EURGBP
// CB = CADJPY
# define PROFIT_TYPE_AU         1
# define PROFIT_TYPE_UB         2
# define PROFIT_TYPE_AC         3
# define PROFIT_TYPE_CB         4

# define SWAP_CALC_MONEY        1
# define SWAP_CALC_USD          2

# define PREC_INT               0
# define PREC_DEFAULT           2
# define PREC_SWAP              4
# define PREC_PRICE             8

struct group {
    char            *name;
    int             leverage;
};

struct fee {
    char            *symbol;
    char            *group;
    mpd_t           *percentage;
    mpd_t           *fee;
    mpd_t           *swap_long;
    mpd_t           *swap_short;
};

struct symbol {
    char            *name;
    char            *security;
    char            *currency;
    int             digit;
    mpd_t           *contract_size;
    mpd_t           *percentage;
    mpd_t           *tick_size;
    mpd_t           *tick_price;
    int             margin_type;
    int             profit_type;
    int             margin_calc;
    int             profit_calc;
    char            *margin_symbol;
    char            *profit_symbol;
    int             swap_calc;
    char            *monday;
    char            *tuesday;
    char            *wednesday;
    char            *thursday;
    char            *friday;
    mpd_t           *c; // c = contract_size / 100
};

struct configs {
    size_t          group_num;
    struct group    *groups;
    size_t          fee_num;
    struct fee      *fees;
    size_t          symbol_num;
    struct symbol   *symbols;
};

extern struct configs configs;
typedef struct symbol symbol_t;

int init_symbol(void);
int group_leverage(const char *group);
int get_weekday(int timezone_offset);
bool is_time_in_range(const char *time_str, const char *range_str);
bool check_time_in_range(const char *time_range, int timezone_offset);

mpd_t* symbol_percentage(const char *group, const char *symbol);
mpd_t* symbol_fee(const char *group, const char *symbol);
mpd_t* symbol_swap_long(const char *group, const char *symbol);
mpd_t* symbol_swap_short(const char *group, const char *symbol);
symbol_t *get_symbol(const char *name);

# endif

