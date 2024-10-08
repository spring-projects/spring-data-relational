#!/bin/bash -x

set -euo pipefail

export JENKINS_USER=${JENKINS_USER_NAME}

MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml -Dscan=false clean -Dmaven.repo.local=/tmp/jenkins-home/.m2/spring-data-jdbc -Ddevelocity.storage.directory=/tmp/jenkins-home/.develocity-root
