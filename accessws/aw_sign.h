# ifndef _AW_SIGN_H_
# define _AW_SIGN_H_

int init_sign(void);

int send_sign_request(nw_ses *ses, uint64_t id, struct clt_info *info, json_t *params);

# endif

