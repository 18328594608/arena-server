/*
 * Description: 
 *     History: Fly, 2022/06/13, create
 */

#ifndef _SHA1_H
#define _SHA1_H

#include <stdint.h>
#include <stddef.h>

struct sha1_ctx {
    uint32_t state[5];
    size_t count[2];
    uint8_t buffer[64];
};

void sha1_init(struct sha1_ctx *ctx);
void sha1_update(struct sha1_ctx *ctx, const void *data, size_t len);
void sha1_final(struct sha1_ctx *ctx, uint8_t digest[20]);

#endif
