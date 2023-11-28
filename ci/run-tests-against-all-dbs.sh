#!/bin/sh

export DEVELOCITY_CACHE_USERNAME=${DEVELOCITY_CACHE_USR}
export DEVELOCITY_CACHE_PASSWORD=${DEVELOCITY_CACHE_PSW}
export JENKINS_USER=${JENKINS_USER_NAME}

# The environment variable to configure access key is still GRADLE_ENTERPRISE_ACCESS_KEY
export GRADLE_ENTERPRISE_ACCESS_KEY=${DEVELOCITY_ACCESS_KEY}

MAVEN_OPTS="-Duser.name=${JENKINS_USER} -Duser.home=/tmp/jenkins-home" \
  ./mvnw -s settings.xml clean install -Pall-dbs
