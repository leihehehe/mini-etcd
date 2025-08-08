protoc -I api/proto/def \
  --go_out=api/proto --go_opt=paths=source_relative \
  --go-grpc_out=api/proto --go-grpc_opt=paths=source_relative \
  $(find api/proto/def -name "*.proto")