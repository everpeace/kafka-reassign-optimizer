.PHONY: test release clean

all: publishLocal

publishLocal: test
	sbt publishLocal

clean:
	sbt clean

test:
	sbt ';set javaOptions in Test += "-Djava.library.path=/usr/local/lib"; test'

release: test
	sbt "release skip-tests"
