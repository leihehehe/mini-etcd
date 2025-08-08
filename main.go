package main

import (
	"context"
	"google.golang.org/grpc"
	"log"
	"net"
)
import (
	pb "mini-etcd/api/proto"
)

type kvServer struct {
	pb.UnimplementedKVServer
}

// Put TODO: Refactor to make it work(currently just a mock moethd)
func (s *kvServer) Put(ctx context.Context, in *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("put: %v", in)

	resp := &pb.PutResponse{
		Header: &pb.ResponseHeader{
			Revision: 1, // todo: this is just a temp value
		},
	}

	return resp, nil
}

func main() {
	s := grpc.NewServer()
	pb.RegisterKVServer(s, &kvServer{})
	lst, err := net.Listen("tcp", ":2379")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("listening on %v", lst)
	if err := s.Serve(lst); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
