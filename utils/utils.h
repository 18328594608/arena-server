/*
 * Description: 
 *     History: Fly, 2022/06/13, create
 */

#ifndef _UTILS_H
#define _UTILS_H

#include <stddef.h>
#include <stdbool.h>
#include <inttypes.h>

#ifndef container_of
#define container_of(ptr, type, member)                 \
    ({                              \
        const __typeof__(((type *) NULL)->member) *__mptr = (ptr);  \
        (type *) ((char *) __mptr - offsetof(type, member));    \
    })
#endif

int get_nonce(uint8_t *dest, int len);
int parse_url(const char *url, char *host, int host_len,
    int *port, const char **path, bool *ssl);

int tcp_connect(const char *host, int port, int flags, bool *inprogress, int *eai);

int b64_encode(const void *src, size_t srclen, void *dest, size_t destsize);

#endif
