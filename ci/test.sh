#!/bin/bash -x

set -euo pipefail

ci/accept-third-party-license.sh

echo "Copying ProxyImageNameSubstitutor into JDBC and R2DBC..."
cp spring-data-relational/src/test/java/org/springframework/data/ProxyImageNameSubstitutor.java spring-data-jdbc/src/test/java/org/springframework/data
cp spring-data-relational/src/test/java/org/springframework/data/ProxyImageNameSubstitutor.java spring-data-r2dbc/src/test/java/org/springframework/data

mkdir -p /tmp/jenkins-home
chown -R 1001:1001 .
MAVEN_OPTS="-Duser.name=jenkins -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml \
  -P${PROFILE} clean dependency:list verify -Dsort -U -B -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-jdbc
