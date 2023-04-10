# ifndef _UT_CRC32_H_
# define _UT_CRC32_H_

# include <stddef.h>
# include <stdint.h>

uint32_t generate_crc32c(const char *string, size_t length);

# endif
