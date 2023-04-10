# include "me_config.h"
# include "me_balance.h"

dict_t *dict_balance;

static uint32_t balance_dict_hash_function(const void *key)
{
    return dict_generic_hash_function(key, sizeof(struct balance_key));
}

static void *balance_dict_key_dup(const void *key)
{
    struct balance_key *obj = malloc(sizeof(struct balance_key));
    if (obj == NULL)
        return NULL;
    memcpy(obj, key, sizeof(struct balance_key));
    return obj;
}

static void *balance_dict_val_dup(const void *val)
{
    return mpd_qncopy(val);
}

static int balance_dict_key_compare(const void *key1, const void *key2)
{
    return memcmp(key1, key2, sizeof(struct balance_key));
}

static void balance_dict_key_free(void *key)
{
    free(key);
}

static void balance_dict_val_free(void *val)
{
    mpd_del(val);
}

static int init_dict(void)
{
    dict_types type;
    memset(&type, 0, sizeof(type));
    type.hash_function  = balance_dict_hash_function;
    type.key_compare    = balance_dict_key_compare;
    type.key_dup        = balance_dict_key_dup;
    type.key_destructor = balance_dict_key_free;
    type.val_dup        = balance_dict_val_dup;
    type.val_destructor = balance_dict_val_free;

    dict_balance = dict_create(&type, 64);
    if (dict_balance == NULL)
        return -__LINE__;

    return 0;
}

int init_balance()
{
    ERR_RET(init_dict());
    return 0;
}

mpd_t *balance_get(uint32_t user_id, uint32_t type)
{
    struct balance_key key;
    key.user_id = user_id;
    key.type = type;

    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

void balance_del(uint32_t user_id, uint32_t type)
{
    struct balance_key key;
    key.user_id = user_id;
    key.type = type;
    dict_delete(dict_balance, &key);
}

mpd_t *balance_set(uint32_t user_id, uint32_t type, mpd_t *amount)
{
    int ret = mpd_cmp(amount, mpd_zero, &mpd_ctx);
    if (ret < 0) {
        return NULL;
    } else if (ret == 0) {
        balance_del(user_id, type);
        return mpd_zero;
    }

    struct balance_key key;
    key.user_id = user_id;
    key.type = type;

    mpd_t *result;
    dict_entry *entry;
    entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        return result;
    }

    entry = dict_add(dict_balance, &key, amount);
    if (entry == NULL)
        return NULL;
    result = entry->val;

    return result;
}

mpd_t *balance_add(uint32_t user_id, uint32_t type, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    struct balance_key key;
    key.user_id = user_id;
    key.type = type;

    mpd_t *result;
    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_add(result, result, amount, &mpd_ctx);
        return result;
    }

    return balance_set(user_id, type, amount);
}

mpd_t *balance_sub(uint32_t user_id, uint32_t type, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    mpd_t *result = balance_get(user_id, type);
    if (result == NULL)
        return NULL;
    if (mpd_cmp(result, amount, &mpd_ctx) < 0)
        return NULL;

    mpd_sub(result, result, amount, &mpd_ctx);
    if (mpd_cmp(result, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, type);
        return mpd_zero;
    }

    return result;
}

mpd_t *balance_freeze(uint32_t user_id, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE);
    if (available == NULL)
        return NULL;
    if (mpd_cmp(available, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, BALANCE_TYPE_FREEZE, amount) == 0)
        return NULL;
    mpd_sub(available, available, amount, &mpd_ctx);
    if (mpd_cmp(available, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, BALANCE_TYPE_AVAILABLE);
        return mpd_zero;
    }

    return available;
}

mpd_t *balance_unfreeze(uint32_t user_id, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;
    mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE);
    if (freeze == NULL)
        return NULL;
    if (mpd_cmp(freeze, amount, &mpd_ctx) < 0)
        return NULL;

    if (balance_add(user_id, BALANCE_TYPE_AVAILABLE, amount) == 0)
        return NULL;
    mpd_sub(freeze, freeze, amount, &mpd_ctx);
    if (mpd_cmp(freeze, mpd_zero, &mpd_ctx) == 0) {
        balance_del(user_id, BALANCE_TYPE_FREEZE);
        return mpd_zero;
    }

    return freeze;
}

mpd_t *balance_total(uint32_t user_id)
{
    mpd_t *balance = mpd_new(&mpd_ctx);
    mpd_copy(balance, mpd_zero, &mpd_ctx);
    mpd_t *available = balance_get(user_id, BALANCE_TYPE_AVAILABLE);
    if (available) {
        mpd_add(balance, balance, available, &mpd_ctx);
    }
    mpd_t *freeze = balance_get(user_id, BALANCE_TYPE_FREEZE);
    if (freeze) {
        mpd_add(balance, balance, freeze, &mpd_ctx);
    }

    return balance;
}

int balance_status(mpd_t *total, size_t *available_count, mpd_t *available, size_t *freeze_count, mpd_t *freeze)
{
    *freeze_count = 0;
    *available_count = 0;
    mpd_copy(total, mpd_zero, &mpd_ctx);
    mpd_copy(freeze, mpd_zero, &mpd_ctx);
    mpd_copy(available, mpd_zero, &mpd_ctx);

    dict_entry *entry;
    dict_iterator *iter = dict_get_iterator(dict_balance);
    while ((entry = dict_next(iter)) != NULL) {
        struct balance_key *key = entry->key;
        mpd_add(total, total, entry->val, &mpd_ctx);
        if (key->type == BALANCE_TYPE_AVAILABLE) {
            *available_count += 1;
            mpd_add(available, available, entry->val, &mpd_ctx);
        } else {
            *freeze_count += 1;
            mpd_add(freeze, freeze, entry->val, &mpd_ctx);
        }
    }
    dict_release_iterator(iter);

    return 0;
}

mpd_t *balance_get_v2(uint64_t sid, uint32_t type)
{
    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;

    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        return entry->val;
    }

    return NULL;
}

void balance_del_v2(uint64_t sid, uint32_t type)
{
    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;
    dict_delete(dict_balance, &key);
}

mpd_t *balance_set_v2(uint64_t sid, uint32_t type, mpd_t *amount)
{
    int ret = mpd_cmp(amount, mpd_zero, &mpd_ctx);
    if (ret < 0) {
        return NULL;
    } else if (ret == 0) {
        balance_del_v2(sid, type);
        return mpd_zero;
    }

    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;

    mpd_t *result;
    dict_entry *entry;
    entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        return result;
    }

    entry = dict_add(dict_balance, &key, amount);
    if (entry == NULL)
        return NULL;
    result = entry->val;

    return result;
}

mpd_t *balance_add_v2(uint64_t sid, uint32_t type, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;

    mpd_t *result;
    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        result = entry->val;
        mpd_add(result, result, amount, &mpd_ctx);
        return result;
    }

    return balance_set_v2(sid, type, amount);
}

mpd_t *balance_sub_v2(uint64_t sid, uint32_t type, mpd_t *amount)
{
    if (mpd_cmp(amount, mpd_zero, &mpd_ctx) < 0)
        return NULL;

    mpd_t *result = balance_get_v2(sid, type);
    if (result == NULL)
        return NULL;
    if (mpd_cmp(result, amount, &mpd_ctx) < 0)
        return NULL;

    mpd_sub(result, result, amount, &mpd_ctx);
    if (mpd_cmp(result, mpd_zero, &mpd_ctx) == 0) {
        balance_del_v2(sid, type);
        return mpd_zero;
    }

    return result;
}

mpd_t *balance_set_float(uint64_t sid, uint32_t type, mpd_t *amount)
{
    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;

    mpd_t *result;
    dict_entry *entry = dict_add(dict_balance, &key, amount);
    if (entry == NULL)
        return NULL;

    return entry->val;
}

mpd_t *balance_get_float(uint64_t sid, uint32_t type)
{
    struct balance_key key;
    key.user_id = 0;
    key.sid1 = sid / 100;
    key.sid2 = sid % 100;
    key.type = type;

    dict_entry *entry = dict_find(dict_balance, &key);
    if (entry) {
        return entry->val;
    } else {
        return NULL;
    }
}

mpd_t *balance_add_float(uint64_t sid, uint32_t type, mpd_t *amount)
{
    mpd_t *result = balance_get_float(sid, type);
    if (result) {
        mpd_add(result, result, amount, &mpd_ctx);
    } else {
        result = balance_set_float(sid, type, amount);
    }
    return result;
}

mpd_t *balance_sub_float(uint64_t sid, uint32_t type, mpd_t *amount)
{
    mpd_t *result = balance_get_float(sid, type);
    if (result) {
        mpd_sub(result, result, amount, &mpd_ctx);
    } else {
	mpd_minus(result, amount, &mpd_ctx);
    }
    return result;
}
