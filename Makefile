.PHONY: test release clean publishLocal local-kafka-cluster

num ?= 3
brokers ?= 1,2,3
library_path ?= /usr/local/lib

all: publishLocal

publishLocal: test
	sbt publishLocal

clean:
	sbt clean

test:
	sbt ';set javaOptions in Test += "-Djava.library.path=$(library_path)"; test'

release: test
	sbt "release skip-tests"

local-kafka-cluster:
	kafakas=() && for i in $$(seq 1 $(num)); do kafkas+=("kafka"$$i); done \
	&& docker-compose up -d "$${kafkas[@]}" \
	&& sleep 2 \
	&& docker-compose exec kafka1 sh -c \
	'kafka-topics --zookeeper $$ZOOKEEPER --create --if-not-exists --topic tp1 --partitions 2 --replication-factor 3'

run:
	sbt ';set javaOptions in run += "-Djava.library.path=$(library_path)"; run --zookeeper localhost:2181 --brokers $(brokers)'
