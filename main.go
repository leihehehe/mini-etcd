package main

import (
	"fmt"
	"log"
	"mini-etcd/internal"
	pb "mini-etcd/proto"
	api "mini-etcd/service"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

func main() {
	kvServer := api.NewKVServer()
	s := grpc.NewServer()
	pb.RegisterKVServer(s, kvServer)
	lst, err := net.Listen("tcp", fmt.Sprintf(":%d", internal.DefaultPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Printf("mini-etcd server listening at %v", lst.Addr())

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("shutting down")
		kvServer.Close()
		s.GracefulStop()
		os.Exit(0)
	}()
	if err := s.Serve(lst); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
