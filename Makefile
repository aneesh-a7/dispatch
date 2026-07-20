.PHONY: build run-controlplane run-worker vet fmt test clean

build:
	go build -o bin/controlplane ./cmd/controlplane
	go build -o bin/worker ./cmd/worker
	go build -o bin/dispatchctl ./cmd/dispatchctl

run-controlplane:
	go run ./cmd/controlplane

run-worker:
	go run ./cmd/worker

vet:
	go vet ./...

fmt:
	gofmt -l .

test:
	go test ./...

clean:
	rm -rf bin/ data/
