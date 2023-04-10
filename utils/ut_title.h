# ifndef _UT_TITLE_H_
# define _UT_TITLE_H_

/* should call this function in the beginning of main */
void process_title_init(int argc, char *argv[]);

/* update process titile */
void process_title_set(const char *fmt, ...);

# endif

