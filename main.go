package main

import (
	"google.golang.org/grpc"
	"log"
	pb "mini-etcd/proto"
	api "mini-etcd/service"
	"net"
)

func main() {
	kvServer := api.NewKVServer()
	s := grpc.NewServer()
	pb.RegisterKVServer(s, kvServer)
	lst, err := net.Listen("tcp", ":2379")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("mini-etcd server listening at %v", lst.Addr())
	if err := s.Serve(lst); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
