#!/bin/bash

MYSQL_HOST="localhost"
MYSQL_USER="root"
MYSQL_PASS="123"
MYSQL_DB="history"

for i in `seq 0 99`
do
    echo "create table balance_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "CREATE TABLE balance_history_$i LIKE balance_history_example;"
done

for i in `seq 0 99`
do
    echo "create table order_history_$i"
    mysql -h$MYSQL_HOST -u$MYSQL_USER -p$MYSQL_PASS $MYSQL_DB -e "CREATE TABLE order_history_$i LIKE order_history_example;"
done
