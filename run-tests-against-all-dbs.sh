#!/bin/sh

./start-all-dbs.sh && mvn clean install -Pall-dbs && ./stop-all-dbs.sh
