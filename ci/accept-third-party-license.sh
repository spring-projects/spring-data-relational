#!/bin/sh

{
  echo "mcr.microsoft.com/mssql/server:2019-CU16-ubuntu-20.04"
  echo "ibmcom/db2:11.5.7.0a"
} > spring-data-jdbc/src/test/resources/container-license-acceptance.txt
