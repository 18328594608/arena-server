TARGET  := accessws.exe
INCS = -I ../network -I ../utils
LIBS = -L ../utils -lutils -L ../network -lnetwork -L ../depends/hiredis -Wl,-Bstatic -lev -ljansson -lmpdec -lrdkafka -lz -lssl -lcrypto -lhiredis -Wl,-Bdynamic -lm -lpthread -llz4 -ldl -lldap -llber -lgss -lgnutls -lidn -lcurl -lnettle -lrtmp -lmysqlclient
include ../makefile.inc
