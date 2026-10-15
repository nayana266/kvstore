MODULE := kvstore
PROTO_DIR := proto

.PHONY: proto build test

proto:
	protoc --proto_path=$(PROTO_DIR) \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTO_DIR)/*.proto

build:
	go build ./...

test:
	go test -race ./...
