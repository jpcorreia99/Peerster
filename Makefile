export GLOG = warn
export BINLOG = warn
export HTTPLOG = warn

test: test_hw0 test_hw1 test_hw2 test_hw3

xtest: setbin test

setbin:
	GOOS=darwin GOARCH=amd64 go build -o ./peer/tests/integration/node.darwin.amd64 ./gui/; \
	GOOS=linux GOARCH=amd64 go build -o ./peer/tests/integration/node.linux.amd64 ./gui/;

test_hw0: test_unit_hw0 test_int_hw0
test_hw1: test_unit_hw1 test_int_hw1
test_hw2: test_unit_hw2 test_int_hw2
test_hw3: test_unit_hw3 test_int_hw3

test_unit_hw0:
	go test -v -race -run Test_HW0 ./peer/tests/unit

test_unit_hw1:
	go test -v -race -run Test_HW1 ./peer/tests/unit

test_unit_hw2:
	go test -v -race -run Test_HW2 ./peer/tests/unit

test_unit_hw3:
	go test -v -race -run Test_HW3 ./peer/tests/unit

test_int_hw0:
	go test -timeout 40m -v -race -run Test_HW0 ./peer/tests/integration

test_int_hw1:
	go test -timeout 40m -v -race -run Test_HW1 ./peer/tests/integration

test_int_hw2:
	go test -timeout 5m -v -race -run Test_HW2 ./peer/tests/integration

test_int_hw3:
	go test -timeout 5m -v -race -run Test_HW3 ./peer/tests/integration

lint:
	# Coding style static check.
	@go get -v honnef.co/go/tools/cmd/staticcheck
	@go mod tidy
	staticcheck ./...

vet:
	go vet ./...
