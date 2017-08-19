#!/usr/bin/env bash
set -ex

sbt ';set javaOptions in Test += "-Djava.library.path=/usr/local/lib"; test'
sbt "release skip-tests"
