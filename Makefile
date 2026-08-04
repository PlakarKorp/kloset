GO =	go

all:
	@echo "nothing to build, kloset is a library."
	@echo "run ${MAKE} test for tests and vet."

check: test

test:
	${GO} test ./...
	${GO} vet ./...

junit:
	${GO} test -v -p 4 -coverprofile=coverage.out -covermode=atomic -timeout 2m -json ./... \
		| ${GO} tool go-junit-report -parser gojson -set-exit-code > junit.xml

.PHONY: all check test junit
