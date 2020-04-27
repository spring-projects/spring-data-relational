#!/bin/sh

{
  echo "mcr.microsoft.com/mssql/server:2017-CU12"
  echo "ibmcom/db2:11.5.0.0a"
} > spring-data-jdbc/src/test/resources/container-license-acceptance.txt