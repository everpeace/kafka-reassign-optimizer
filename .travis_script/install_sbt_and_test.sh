#!/usr/bin/env bash
set -xe

# install sbt
apt-get update
apt-get install -y gnupg apt-transport-https
echo 'deb https://dl.bintray.com/sbt/debian /' | tee -a /etc/apt/sources.list.d/sbt.list
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
apt-get update
apt-get install -y sbt

# do test
cd /kafka-reassign-optimizer
sbt ';set javaOptions in Test += "-Djava.library.path=/usr/lib/lp_solve"; test'
