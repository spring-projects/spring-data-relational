#!/bin/bash -x

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

rm -rf $HOME/.m2/repository/org/springframework/data 2> /dev/null || :

cd spring-data-jdbc-github

./mvnw clean dependency:list test -Dsort -U -P${PROFILE}
