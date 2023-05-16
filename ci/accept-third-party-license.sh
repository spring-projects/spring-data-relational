#!/bin/sh

{
  echo "mcr.microsoft.com/mssql/server:2019-CU16-ubuntu-20.04"
  echo "ibmcom/db2:11.5.7.0a"
  echo "harbor-repo.vmware.com/dockerhub-proxy-cache/ibmcom/db2:11.5.7.0a"
} > spring-data-jdbc/src/test/resources/container-license-acceptance.txt

{
  echo "mcr.microsoft.com/mssql/server:2022-latest"
  echo "ibmcom/db2:11.5.7.0a"
  echo "harbor-repo.vmware.com/dockerhub-proxy-cache/ibmcom/db2:11.5.7.0a"
} > spring-data-r2dbc/src/test/resources/container-license-acceptance.txt
