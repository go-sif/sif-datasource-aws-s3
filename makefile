version=0.1.0
export GOPROXY=direct

.PHONY: all dependencies serve-docs clean test lint fmt watch testdata start-testenv stop-testenv

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  dependencies  - install dependencies"
	@echo "  build         - build the source code"
	@echo "  watch         - watch and build the source code"
	@echo "  serve-docs    - serve the documentation"
	@echo "  clean         - clean the source directory"
	@echo "  lint          - lint the source code"
	@echo "  fmt           - format the source code"
	@echo "  testdata      - download test data
	@echo "  start-testenv - start the test environment (localstack)"
	@echo "  stop-testenv  - stop the test environment"
	@echo "  test          - test the source code"

dependencies:
	@go get -u golang.org/x/tools
	@go get -u golang.org/x/lint/golint
	@go get -u golang.org/x/tools/cmd/godoc
	@go get -u github.com/unchartedsoftware/witch
	@go get -d -v ./...

fmt:
	@go fmt ./...

clean:
	@rm -rf ./bin

lint:
	@echo "Running go vet"
	@go vet ./...
	@echo "Running golint"
	@go list ./... | grep -v /vendor/ | xargs -L1 golint --set_exit_status

testdata:
	@echo "Downloading EDSM test files..."
	@mkdir -p testdata
	@cd testdata && curl -s https://www.edsm.net/dump/systemsWithCoordinates7days.json.gz | gunzip | tail -n +2 | head -n -1 | sed 's/,$$//' | sed 's/^....//' | split --additional-suffix .jsonl -l 50000
	@echo "Finished downloading EDSM test files."

start-testenv:
	@docker run -d -v /var/run/docker.sock:/var/run/docker.sock -e LOCALSTACK_SERVICES=s3 -p 4566:4566 --name localstack localstack/localstack:latest

stop-testenv:
	@docker rm -fv localstack

test: build
	@echo "Running tests..."
	@go test -short -count=1 ./...

cover: build
	@echo "Running tests with coverage..."
	@go test -coverprofile=cover.out -coverpkg=./... ./...
	@go tool cover -html=cover.out -o cover.html

generate:
	@echo "Generating protobuf code..."
	@go generate ./...
	@echo "Finished generating protobuf code."

build: generate lint
	@echo "Building sif-datasource-aws-s3..."
	@go build ./...
	@go mod tidy
	@echo "Finished building sif-datasource-aws-s3."

serve-docs:
	@echo "Serving docs on http://localhost:6060"
	@witch --cmd="godoc -http=localhost:6060" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner

watch:
	@witch --cmd="make build" --watch="**/*.go" --ignore="vendor,.git,**/*.pb.go" --no-spinner
