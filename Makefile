.PHONY: all test compile start-antidote-docker stop-antidote-docker mount-fs mount-fs-cleanup run

GRADLE := $(shell pwd)/gradlew

ANTIDOTE_DOCKER_CONTAINERS := $(shell docker ps -a -q -f ancestor=antidotedb/antidote)

all: compile

compile:
	$(GRADLE) :compileJava

test:
	$(GRADLE) check


start-antidote-docker:
	docker run -d --rm -it -p "8087:8087" antidotedb/antidote

stop-antidote-docker:
	docker rm -f $(ANTIDOTE_DOCKER_CONTAINERS)

mount-fs: 
	$(GRADLE) run -Dexec.args="-d d1 -a 127.0.0.1:8087"

# trap the Ctrl+C (INT) to stop the Antidote container
mount-fs-cleanup:
	bash -c "trap \"docker rm -f $$(docker ps -a -q -f ancestor=antidotedb/antidote)\" INT; ./gradlew run -Dexec.args=\"-d d1 -a 127.0.0.1:8087\""

run: start-antidote-docker mount-fs-cleanup
