### Ubuntu 16.04

#### 1.depends
```
sudo apt-get update
sudo apt install -y libjansson-dev libmpdec-dev libmysqlclient-dev libcurl4-gnutls-dev libldap2-dev libgss-dev librtmp-dev liblz4-dev
sudo apt-get install libev-dev
```

#### 2.libkafka
```
git clone https://github.com/edenhill/librdkafka.git
cd librdkafka/
./configure
make
sudo make install
```

#### 3.viabtc
```
git clone https://github.com/gutongjiang/viabtc_exchange_server.git

cd depends/hiredis/
make
sudo make install

cd network/
make

cd utils/
make

cd matchengine/
make
```

#### 4.mysql
```
sudo apt-get install mysql-server
(set root password)

create database config;
create database log;
create database history;
```

#### 5.kafka
```
sudo apt-get install openjdk-8-jdk
java -version

wget https://archive.apache.org/dist/kafka/3.1.0/kafka-3.1.0-src.tgz
tar -xzvf kafka-3.1.0-src.tgz
cd kafka-3.1.0-src
./gradlew jar -PscalaVersion=2.13.6

// start
./zookeeper-server-start.sh -daemon ../config/zookeeper.properties
./kafka-server-start.sh -daemon ../config/server.properties

// create
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
./kafka-topics.sh --create --bootstrap-server localhost:2181 --replication-factor 1 --partitions 1 --topic test

./kafka-topics.sh --list --zookeeper localhost:2181
./kafka-topics.sh --list --bootstrap-server localhost:2181

// producer & consumer
./kafka-console-producer.sh --broker-list localhost:9092 --topic test
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

#### 6.Ubuntu 18.04
```
// makefile.inc

SOURCE  := $(wildcard *.c)
OBJS    := $(patsubst %.c, %.o, $(SOURCE))
CC      := gcc
CFLAGS  := -Wall -Wno-strict-aliasing -Wno-uninitialized -g -rdynamic -std=gnu99
LFLAGS  := -g -rdynamic

.PHONY : all clean install

all : $(TARGET)

clean :
        rm -rf *.d *.o $(TARGET)

$(TARGET) : $(OBJS)
        $(CC) $(LFLAGS) -no-pie -o $@ $(OBJS) $(LIBS)
.c.o :
        $(CC) $(CFLAGS) -no-pie -c -o $@ $< $(INCS)

install :
```
