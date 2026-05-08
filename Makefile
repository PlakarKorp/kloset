GO =	go

all:
	@echo "nothing to build, kloset is a library."
	@echo "run ${MAKE} test for tests and vet."

check: test

test:
	${GO} test ./...
	${GO} vet ./...

.PHONY: all check test
