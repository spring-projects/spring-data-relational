#!/bin/sh

{
  echo "mcr.microsoft.com/mssql/server:2022-latest"
  echo "ibmcom/db2:11.5.7.0a"
  echo "docker-hub.usw1.packages.broadcom.com/mssql/server:2022-latest"
  echo "docker-hub.usw1.packages.broadcom.com/ibmcom/db2:11.5.7.0a"
} > spring-data-jdbc/src/test/resources/container-license-acceptance.txt

{
  echo "mcr.microsoft.com/mssql/server:2022-latest"
  echo "ibmcom/db2:11.5.7.0a"
  echo "docker-hub.usw1.packages.broadcom.com/mssql/server:2022-latest"
  echo "docker-hub.usw1.packages.broadcom.com/ibmcom/db2:11.5.7.0a"
} > spring-data-r2dbc/src/test/resources/container-license-acceptance.txt
