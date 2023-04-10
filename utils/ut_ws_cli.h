/*
 * Description: 
 *     History: Fly, 2022/06/13, create
 */

# ifndef _UT_WS_CLI_H_
# define _UT_WS_CLI_H_

# include "uwsc.h"

typedef struct uwsc_client ws_cli; 

ws_cli *ws_cli_create(const char *url, void *open, void* message, void *error, void *close);
void ws_cli_release(ws_cli *cli);

# endif

