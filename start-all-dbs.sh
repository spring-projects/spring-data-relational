#!/bin/sh

# postgres
pg_ctl -D /usr/local/var/postgres start

# mysql
mysql.server start
