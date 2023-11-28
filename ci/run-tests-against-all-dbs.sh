#!/bin/sh

export JENKINS_USER=${JENKINS_USER_NAME}

MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml clean install -Pall-dbs
