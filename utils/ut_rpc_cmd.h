# ifndef _UT_RPC_CMD_H_
# define _UT_RPC_CMD_H_

// config
# define CMD_GROUP_LIST             91
# define CMD_SYMBOL_LIST            92
# define CMD_TICK_STATUS            93

// balance
# define CMD_BALANCE_QUERY          101
# define CMD_BALANCE_UPDATE         102
# define CMD_BALANCE_HISTORY        103

// trade
# define CMD_ORDER_PUT_LIMIT        201
# define CMD_ORDER_PUT_MARKET       202
# define CMD_ORDER_QUERY            203
# define CMD_ORDER_BOOK             205
# define CMD_ORDER_BOOK_DEPTH       206
# define CMD_ORDER_DETAIL           207
# define CMD_ORDER_DEALS            209
# define CMD_ORDER_DETAIL_FINISHED  210

# define CMD_ORDER_OPEN             211
# define CMD_ORDER_CLOSE            212
# define CMD_ORDER_POSITION         213
# define CMD_ORDER_UPDATE           214
# define CMD_ORDER_LIMIT            215
# define CMD_ORDER_CANCEL           216
# define CMD_ORDER_PENDING          217
# define CMD_ORDER_HISTORY          218

//EXTERNAL USE
# define CMD_ORDER_CLOSE_EXTERNAL   220
# define CMD_ORDER_UPDATE_EXTERNAL  221
# define CMD_ORDER_CANCEL_EXTERNAL  222
# define CMD_CHANGE_EXTERNAL        223

# define CMD_ORDER_OPEN2            230
# define CMD_ORDER_CLOSE2           231

// market
# define CMD_MARKET_STATUS          301
# define CMD_MARKET_KLINE           302
# define CMD_MARKET_DEALS           303
# define CMD_MARKET_LAST            304
# define CMD_MARKET_STATUS_TODAY    305
# define CMD_MARKET_USER_DEALS      306
# define CMD_MARKET_LIST            307
# define CMD_MARKET_SUMMARY         308

# endif

