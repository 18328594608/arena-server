/*
 * Description: 
 *     History: Fly, 2022/06/13, create
 */

# include "ut_ws_cli.h"

struct ev_loop *nw_default_loop2;

ws_cli *ws_cli_create(const char *url, void *open, void* message, void *error, void *close)
{
    int ping_interval = 10;
    ws_cli *cli = uwsc_new(nw_default_loop2, url, ping_interval, NULL);
    if (!cli)
        return NULL;

    cli->onopen = open;
    cli->onmessage = message;
    cli->onerror = error;
    cli->onclose = close;

    return cli;
}

void ws_cli_release(ws_cli *cli)
{
    free(cli);
}
