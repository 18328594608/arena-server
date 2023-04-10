/*
 * Description: 
 *     History: Fly, 2022/06/13, create
 */

#ifndef __SSL_H
#define __SSL_H

#include <stdbool.h>

enum {
    SSL_OK = 0,
    SSL_ERROR = -1,
    SSL_PENDING = -2
};

extern int ssl_err_code;

struct ssl_context;

char *ssl_strerror(int error, char *buffer, int len);

struct ssl_context *ssl_context_new(bool server);
void ssl_context_free(struct ssl_context *ctx);

void *ssl_session_new(struct ssl_context *ctx, int sock);
void ssl_session_free(void *ssl);

int ssl_load_ca_crt_file(struct ssl_context *ctx, const char *file);
int ssl_load_crt_file(struct ssl_context *ctx, const char *file);
int ssl_load_key_file(struct ssl_context *ctx, const char *file);

int ssl_set_ciphers(struct ssl_context *ctx, const char *ciphers);

int ssl_set_require_validation(struct ssl_context *ctx, bool require);

void ssl_set_server_name(void *ssl, const char *name);

int ssl_read(void *ssl, void *buf, int len);
int ssl_write(void *ssl, const void *buf, int len);

int ssl_connect(void *ssl, bool server,
        void (*on_verify_error)(int error, const char *str, void *arg), void *arg);

#endif
