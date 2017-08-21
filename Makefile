.PHONY: test release clean publishLocal local-kafka-cluster run_producer stop_producer run

num ?= 3
brokers ?= 1,2,3
library_path ?= /usr/local/lib
topic ?= tp1
partitions ?= 2
replication_factor ?=3

all: publishLocal

publishLocal: test
	sbt docker:publishLocal

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
	'kafka-topics --zookeeper $$ZOOKEEPER --create --if-not-exists --topic $(topic) --partitions $(partitions) --replication-factor $(replication_factor)'

run_producer:
	TOPIC=$(topic) docker-compose up -d producer

stop_producer:
	TOPIC=$(topic) docker-compose stop producer

run:
	sbt ';set javaOptions in run += "-Djava.library.path=$(library_path)"; run --zookeeper localhost:2181 --brokers $(brokers)'
