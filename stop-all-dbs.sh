#!/bin/sh

# postgres
pg_ctl -D /usr/local/var/postgres stop

# mysql
mysql.server stop
